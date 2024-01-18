package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.tools.{JobUtilsService, OmopTools, PGTool}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuiteLike
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import fr.aphp.id.eds.requester.{ResultColumn, SolrCollection, SolrColumn}
import org.apache.solr.client.solrj.SolrRequest
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.{SolrDocument, SolrDocumentList}
import org.apache.spark.sql.functions.{col, explode}

import scala.io.Source

class QueryBuilderTest extends AnyFunSuiteLike with DatasetComparer {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  def testCaseEvaluate(
      folderCase: String,
      withOrganizationsDetail: Boolean = false,
      checkOrder: Boolean = true,
      setUpSolrClientMock: (org.apache.solr.client.solrj.impl.CloudSolrClient) => Unit = (_) => {})
    : DataFrame = {
    val solrQueryResolver: SolrQueryResolver = mock[SolrQueryResolver]
    val expected = getClass.getResource(s"/testCases/$folderCase/expected.csv")
    val expectedResult = sparkSession.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .load(expected.getPath)

    // we don't care about closing the input stream since the jvm will close after testing
    val request = QueryParser.parse(
      Source
        .fromFile(getClass.getResource(s"/testCases/$folderCase/request.json").getFile)
        .getLines
        .mkString,
      QueryParsingOptions(withOrganizationDetails = withOrganizationsDetail)
    )

    val solrClient = mock[org.apache.solr.client.solrj.impl.CloudSolrClient]
    when(solrQueryResolver.getSolrClient(ArgumentMatchersSugar.*)).thenReturn(solrClient)
    setUpSolrClientMock(solrClient)

    val folder = getClass.getResource(s"/testCases/$folderCase").getPath
    new java.io.File(folder).listFiles
      .filter(_.getName.startsWith("resource_"))
      .foreach((f) => {
        val resourceContent = sparkSession.read
          .format("csv")
          .option("delimiter", ";")
          .option("header", "true")
          .load(f.getPath)
        when(
          solrQueryResolver.getSolrResponseDataFrame(ArgumentMatchersSugar.*,
                                                     ArgumentMatchersSugar.*,
                                                     ArgumentMatchersSugar.*)(
            ArgumentMatchersSugar.*,
            ArgumentMatchersSugar.*,
            ArgumentMatchersSugar.eqTo(
              f.getName.replace("resource_", "").replace(".csv", "").toInt.toShort))).thenReturn(
          resourceContent
        )
      })
    val jobUtilsService = mock[JobUtilsService]
    when(jobUtilsService.getRandomIdNotInTabooList(ArgumentMatchersSugar.*)).thenReturn(-10, 99)

    val result = new DefaultQueryBuilder(jobUtilsService).processRequest(
      sparkSession,
      Map("zkhost" -> "dummy"),
      request._1,
      request._2,
      new OmopTools(mock[PGTool], Map()),
      "",
      false,
      withOrganizationsDetail,
      new QueryBuilderGroup(new QueryBuilderBasicResource(querySolver = solrQueryResolver),
                            jobUtilsService = jobUtilsService)
    )
    assertSmallDatasetEquality(result, expectedResult, orderedComparison = checkOrder)
    result
  }

  test("testProcessRequestSimple") {
    testCaseEvaluate("simple")
    testCaseEvaluate("exclusion")
    testCaseEvaluate("occurences")
    testCaseEvaluate("dateRanges")
    testCaseEvaluate("temporalConstraintSameEncounter")
    testCaseEvaluate("temporalConstraintDirectChronologicalOrder")
  }

  test("withNonInclusiveRoot") {
    testCaseEvaluate(
      "withRootNegation",
      setUpSolrClientMock = (solrClient) => {
        val queryResponse = mock[QueryResponse]
        val docList = new SolrDocumentList()
        List(2, 3, 6, 7)
          .map(
            (x) => {
              val doc = new SolrDocument()
              doc.setField(SolrColumn.PATIENT, x)
              doc
            }
          )
          .foreach(x => docList.add(x))
        when(queryResponse.getResults).thenReturn(docList)
        when(
          solrClient.query(ArgumentMatchersSugar.eqTo(SolrCollection.PATIENT_APHP),
                           ArgumentMatchersSugar.*,
                           ArgumentMatchersSugar.eqTo(SolrRequest.METHOD.POST)))
          .thenReturn(queryResponse)

      }
    )
  }

  test("temporalConstraints") {
    testCaseEvaluate("temporalConstraintSameEncounterByPairs")
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

}
