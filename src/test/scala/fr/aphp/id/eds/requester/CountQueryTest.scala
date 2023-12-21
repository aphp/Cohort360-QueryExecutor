package fr.aphp.id.eds.requester

import fr.aphp.id.eds.requester.config.JobsConfig
import fr.aphp.id.eds.requester.jobs.{JobEnv, JobType, SparkJobParameter}
import fr.aphp.id.eds.requester.query.QueryBuilder
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.col
import org.mockito.ArgumentMatchersSugar
import org.mockito.Mockito.when
import org.mockito.MockitoSugar.mock
import org.scalatest.funsuite.AnyFunSuiteLike

import java.nio.file.Paths

class CountQueryTest extends AnyFunSuiteLike {

  System.setProperty("config.resource", "application.test.conf")
  val pgpassFile = Paths.get(scala.sys.env("HOME"), ".pgpass").toFile
  if (!pgpassFile.exists()) {
    pgpassFile.createNewFile()
  }

  test("testCallbackUrl") {
    var callbackUrl = JobsConfig.countJob.callbackUrl(SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test", Some("test"), Some("/url")))
    assert(callbackUrl.isDefined)
    assert(callbackUrl.get == "http://django/url")
    callbackUrl = JobsConfig.countJob.callbackUrl(SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test", Some("id")))
    assert(callbackUrl.isDefined)
    assert(callbackUrl.get == "http://django/cohort/dated-measures/id/")
    callbackUrl = JobsConfig.countJob.callbackUrl(SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test"))
    assert(callbackUrl.isEmpty)
  }

  test("testRunJob") {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val queryBuilderMock = mock[QueryBuilder]
    val queryData =
      """
        {"cohortUuid":"ecd89963-ac90-470d-a397-c846882615a6","sourcePopulation":{"caresiteCohortList":[31558]},"_type":"request","request":{"_type":"andGroup","_id":0,"isInclusive":true,"criteria":[{"_type":"basicResource","_id":1,"isInclusive":true,"resourceType":"patientAphp","filterSolr":"fq=gender:f&fq=deceased:false&fq=active:true","filterFhir":"active=true&gender=f&deceased=false&age-day=ge0&age-day=le130"}],"temporalConstraints":[]}}"
      """.stripMargin


    val expected = getClass.getResource(s"/testCases/simple/expected.csv")
    val expectedResult = sparkSession.read.format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .load(expected.getPath)
    when(
      queryBuilderMock.processRequest(
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*
      )
    ).thenReturn(expectedResult)

    val countJob = CountQuery(queryBuilderMock)
    val res = countJob.runJob(
      sparkSession,
      JobEnv("someid", AppConfig.conf),
      SparkJobParameter(
        "testCohort",
        None,
        queryData,
        "someOwnerId",
        mode = JobType.countAll
      )
    )
    assert(res.status == "FINISHED")
    assert(res.data("count") == "2")
  }

  test("testRunCountWithDetails") {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val queryBuilderMock = mock[QueryBuilder]
    val queryData =
      """
        {"cohortUuid":"ecd89963-ac90-470d-a397-c846882615a6","sourcePopulation":{"caresiteCohortList":[31558]},"_type":"request","request":{"_type":"andGroup","_id":0,"isInclusive":true,"criteria":[{"_type":"basicResource","_id":1,"isInclusive":true,"resourceType":"patientAphp","filterSolr":"fq=gender:f&fq=deceased:false&fq=active:true","filterFhir":"active=true&gender=f&deceased=false&age-day=ge0&age-day=le130"}],"temporalConstraints":[]}}"
      """.stripMargin


    val expected = getClass.getResource(s"/testCases/withOrganizationDetails/expected.csv")
    val expectedResult = sparkSession.read.format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .load(expected.getPath)
      .withColumn(ResultColumn.ORGANIZATIONS, functions.split(col(ResultColumn.ORGANIZATIONS), ","))
      .withColumn(ResultColumn.ORGANIZATIONS, col(ResultColumn.ORGANIZATIONS).cast("array<long>"))
    when(
      queryBuilderMock.processRequest(
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*,
        ArgumentMatchersSugar.*
      )
    ).thenReturn(expectedResult)

    val countJob = CountQuery(queryBuilderMock)
    val res = countJob.runJob(
      sparkSession,
      JobEnv("someid", AppConfig.conf),
      SparkJobParameter(
        "testCohort",
        None,
        queryData,
        "someOwnerId",
        mode = JobType.countWithDetails
      )
    )
    assert(res.status == "FINISHED")
    assert(res.data("count") == "3")
    assert(res.extra("7") == "1")
    assert(res.extra("3") == "2")
  }

}

