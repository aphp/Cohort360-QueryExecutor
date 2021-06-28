package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.tools.{OmopTools, PGTool}
import org.apache.spark.sql.SparkSession
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuiteLike
import com.github.mrpowers.spark.fast.tests.DatasetComparer

import scala.io.Source

class QueryBuilderTest extends AnyFunSuiteLike with DatasetComparer {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  def testCaseEvaluate(folderCase: String): Unit = {
    val solrQueryResolver: SolrQueryResolver = mock[SolrQueryResolver]
    val expected = getClass.getResource(s"/testCases/$folderCase/expected.csv")
    val expectedResult = sparkSession.read.format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .load(expected.getPath)

    // we don't care about closing the input stream since the jvm will close after testing
    val request = QueryParser.parse(Source.fromFile(getClass.getResource(s"/testCases/$folderCase/request.json").getFile).getLines.mkString)

    val folder = getClass.getResource(s"/testCases/$folderCase").getPath
    new java.io.File(folder).listFiles.filter(_.getName.startsWith("resource_")).foreach((f) => {
      val resourceContent = sparkSession.read.format("csv")
        .option("delimiter", ";")
        .option("header", "true")
        .load(f.getPath)
      when(solrQueryResolver.getSolrResponseDataFrame(
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*)(ArgumentMatchersSugar.*, ArgumentMatchersSugar.*, ArgumentMatchersSugar.eqTo(f.getName.replace("resource_", "").replace(".csv", "").toInt.toShort))
      ).thenReturn(
        resourceContent
      )
    })

    val result = QueryBuilder.processRequest(
      sparkSession,
      Map(),
      request._1,
      request._2,
      new OmopTools(mock[PGTool], Map()),
      "",
      false,
      new QueryBuilderGroup(new QueryBuilderBasicResource(querySolver = solrQueryResolver))
    )
    assertSmallDatasetEquality(result, expectedResult)
  }

  test("testProcessRequestSimple") {
    testCaseEvaluate("simple")
    testCaseEvaluate("exclusion")
    testCaseEvaluate("occurences")
    testCaseEvaluate("dateRanges")
    testCaseEvaluate("temporalConstraintSameEncounter")
    testCaseEvaluate("temporalConstraintDirectChronologicalOrder")
  }

  test("temporalConstraints"){
    testCaseEvaluate("temporalConstraintSameEncounterByPairs")
  }

}
