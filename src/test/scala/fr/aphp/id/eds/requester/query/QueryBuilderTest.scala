package fr.aphp.id.eds.requester.query

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import fr.aphp.id.eds.requester.query.engine.{
  DefaultQueryBuilder,
  QueryBuilderBasicResource,
  QueryBuilderGroup
}
import fr.aphp.id.eds.requester.query.model.{BasicResource, QueryParsingOptions}
import fr.aphp.id.eds.requester.query.parser.QueryParser
import fr.aphp.id.eds.requester.query.resolver.FhirResourceResolver
import fr.aphp.id.eds.requester.tools.{JobUtilsService, OmopTools, PGTool}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.io.Source

class QueryBuilderTest extends AnyFunSuiteLike with DatasetComparer {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  def testCaseEvaluate(folderCase: String,
                       withOrganizationsDetail: Boolean = false,
                       checkOrder: Boolean = true): DataFrame = {
    val solrQueryResolver: FhirResourceResolver = mock[FhirResourceResolver]
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
          solrQueryResolver.getSolrResponseDataFrame(
            ArgumentMatchersSugar.argThat((res: BasicResource) => res._id == criterionId),
            ArgumentMatchersSugar.*,
            ArgumentMatchersSugar.*)(
            ArgumentMatchersSugar.*
          )).thenReturn(
          resourceContent
        )
      })
    val jobUtilsService = mock[JobUtilsService]
    when(jobUtilsService.getRandomIdNotInTabooList(ArgumentMatchersSugar.*)).thenReturn(-10, 99)

    val result = new DefaultQueryBuilder(jobUtilsService).processRequest(
      sparkSession,
      request._1,
      request._2,
      new OmopTools(mock[PGTool]),
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
      "withRootNegation"
    )
  }

  test("ipp") {
    testCaseEvaluate(
      "ipp"
    )
  }

  test("temporalConstraints") {
    testCaseEvaluate("temporalConstraintSameEncounterByPairs")
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

}
