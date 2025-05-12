package fr.aphp.id.eds.requester.query

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import fr.aphp.id.eds.requester.CountOptionsDetails.CountOptionsDetails
import fr.aphp.id.eds.requester.{CountOptionsDetails, ResultColumn, SolrCollection}
import fr.aphp.id.eds.requester.query.engine.{DefaultQueryBuilder, QueryBuilderBasicResource, QueryBuilderGroup, QueryExecutionOptions}
import fr.aphp.id.eds.requester.query.model.QueryParsingOptions
import fr.aphp.id.eds.requester.query.parser.QueryParser
import fr.aphp.id.eds.requester.query.resolver.ResourceResolver
import fr.aphp.id.eds.requester.query.resolver.solr.{SolrQueryResolver, SolrSparkReader}
import fr.aphp.id.eds.requester.tools.JobUtils.initStageDetails
import fr.aphp.id.eds.requester.tools.{JobUtilsService, SolrTools, StageDetails}
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.SolrDocumentList
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.{AdditionalAnswers, ArgumentMatchersSugar}
import org.mockito.MockitoSugar.{doAnswer, mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

import java.nio.file.Files
import scala.io.Source

class QueryBuilderTest extends AnyFunSuiteLike with DatasetComparer with BeforeAndAfterAll {
  System.setProperty("config.resource", "application.test.conf")
  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
    super.afterAll()
  }

  def testCaseEvaluate(folderCase: String,
                       withOrganizationsDetail: Boolean = false,
                       withStageDetails: Option[CountOptionsDetails] = None,
                       checkOrder: Boolean = true): DataFrame = {
    val solrSparkReader: SolrSparkReader = mock[SolrSparkReader]
    val solrTools: SolrTools = mock[SolrTools]
    val solrClient: CloudSolrClient = mock[CloudSolrClient]
    val solrQueryResp = mock[QueryResponse]
    val solrResult = mock[SolrDocumentList]
    when(solrQueryResp.getResults).thenReturn(solrResult)
    when(solrResult.getNumFound).thenReturn(100)
    when(
      solrClient.query(ArgumentMatchersSugar.eqTo(SolrCollection.PATIENT_APHP),
                       ArgumentMatchersSugar.*)).thenReturn(solrQueryResp)
    when(solrTools.getSolrClient).thenReturn(solrClient)
    val solrQueryResolver: ResourceResolver = new SolrQueryResolver(solrSparkReader, solrTools)

    val expected = getClass.getResource(s"/testCases/$folderCase/expected.csv")
    val expectedResult = sparkSession.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .load(expected.getPath)
    val expectedStageCounts = if (withStageDetails.isDefined) {
      val expectedStageCounts = getClass.getResource(s"/testCases/$folderCase/stageCounts.csv")
      Some(
        sparkSession.read
          .format("csv")
          .option("delimiter", ";")
          .option("header", "true")
          .load(expectedStageCounts.getPath)
      )
    } else {
      None
    }

    // we don't care about closing the input stream since the jvm will close after testing
    val request = QueryParser.parse(
      Source
        .fromFile(getClass.getResource(s"/testCases/$folderCase/request.json").getFile)
        .getLines
        .mkString,
      QueryParsingOptions(solrQueryResolver.getConfig,
                          withOrganizationDetails = withOrganizationsDetail)
    )

    val folder = getClass.getResource(s"/testCases/$folderCase").getPath
    new java.io.File(folder).listFiles
      .filter(_.getName.startsWith("resource_"))
      .foreach((f) => {
        val criterionId = f.getName.replace("resource_", "").replace(".csv", "").toInt.toShort
        val resourceContent = sparkSession.read
          .format("csv")
          .option("delimiter", ";")
          .option("header", "true")
          .load(f.getPath)
        when(
          solrSparkReader.readDf(
            ArgumentMatchersSugar.*,
            ArgumentMatchersSugar.*,
            ArgumentMatchersSugar.*,
            ArgumentMatchersSugar.eqTo(criterionId)
          )(
            ArgumentMatchersSugar.*
          )).thenReturn(
          resourceContent
        )
      })

    val additionalResourceIds = Iterator.iterate(99)(_ + 1)
    val jobUtilsService = mock[JobUtilsService]
    when(
      jobUtilsService.getRandomIdNotInTabooList(ArgumentMatchersSugar.*,
                                                ArgumentMatchersSugar.eqTo(false)))
      .thenAnswer({additionalResourceIds.next().toShort})
    when(
      jobUtilsService.getRandomIdNotInTabooList(ArgumentMatchersSugar.*,
                                                ArgumentMatchersSugar.eqTo(true))).thenReturn(-10)
    when(jobUtilsService.prepareRequest(ArgumentMatchersSugar.*, ArgumentMatchersSugar.*))
      .thenCallRealMethod()

    val stageDetails = if (withStageDetails.isDefined) {
      initStageDetails(Map("details" -> withStageDetails.get), request._1)
    } else { StageDetails(None, None) }
    val result = new DefaultQueryBuilder(jobUtilsService).processRequest(
      sparkSession,
      request._1,
      request._2,
      stageDetails,
      "",
      false,
      withOrganizationsDetail,
      new QueryBuilderGroup(new QueryBuilderBasicResource(querySolver = solrQueryResolver),
                            QueryExecutionOptions(solrQueryResolver.getConfig),
                            jobUtilsService = jobUtilsService)
    )
    assertSmallDatasetEquality(result, expectedResult, orderedComparison = checkOrder)
    if (withStageDetails.isDefined && expectedStageCounts.isDefined) {
      // transform stageCounts to a sequence of tuple key,value
      val stageCountsSeq = if (withStageDetails.get.equals(CountOptionsDetails.ratio)) {
        stageDetails.stageDfs.get
          .filter(_._1 < 99) // remove added virtual groups
          .map { case (k, v) => (k.toString, v.join(result, ResultColumn.SUBJECT).count().toString) }
          .toSeq
      } else {
        stageDetails.stageCounts
          .get
          .map { case (k, v) => (k.toString, v.toString) }
          .toSeq
      }
      assertSmallDatasetEquality(
        sparkSession.createDataFrame(stageCountsSeq).toDF("stage", "count"),
        expectedStageCounts.get, orderedComparison = false)
    }
    result
  }

  test("liveTest") {
    testCaseEvaluate("liveTestTmp")
  }

  test("custom") {
    testCaseEvaluate("testCustom")
  }

  test("simple") {
    testCaseEvaluate("simple")
  }

  test("exclusion") { testCaseEvaluate("exclusion") }
  test("occurences") { testCaseEvaluate("occurences") }
  test("temporalConstraintSameEncounter") { testCaseEvaluate("temporalConstraintSameEncounter") }
  test("temporalConstraintDirectChronologicalOrder") {
    testCaseEvaluate("temporalConstraintDirectChronologicalOrder")
  }

  test("withNonInclusiveRoot") {
    testCaseEvaluate(
      "withRootNegation"
    )
  }

  test("ipp") {
    testCaseEvaluate(
      "ipp"
    )
  }

  test("temporalConstraints") {
    testCaseEvaluate("temporalConstraintSameEncounterByPairs",
                     withStageDetails = Some(CountOptionsDetails.all))
  }

  test("nAmongM") {
    testCaseEvaluate("nAmongM")
  }

  test("questionnaireResponse") {
    testCaseEvaluate("temporalConstraintSameEpisodeOfCare")
  }

  test("resourceCohort") {
    testCaseEvaluate("resourceCohort", checkOrder = false)
  }

  test("withOrganizationDetails") {
    testCaseEvaluate(
      "withOrganizationDetails",
      withOrganizationsDetail = true
    )
  }

  test("stageRatioDetails") {
    testCaseEvaluate("stageRatioDetails", withStageDetails = Some(CountOptionsDetails.ratio))
  }

  test("edgeCases") {
    testCaseEvaluate("edgeCases/temporalConstraintAllSameEncounterWithMultipleGroupLevel")
    testCaseEvaluate("edgeCases/temporalConstraintAppliedToSingleCriteria")
  }

  test("nAmongMUniqueFields") {
    testCaseEvaluate("nAmongMUniqueFields")
  }

  test("patientAge") {
    testCaseEvaluate("patientAge")
  }

}
