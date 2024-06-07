package fr.aphp.id.eds.requester

import fr.aphp.id.eds.requester.config.JobsConfig
import fr.aphp.id.eds.requester.jobs.{JobEnv, SparkJobParameter}
import fr.aphp.id.eds.requester.query.QueryBuilder
import fr.aphp.id.eds.requester.tools.{JobUtilsService, OmopTools}
import org.apache.spark.sql.SparkSession
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuiteLike

import java.nio.file.Paths

class CreateQueryTest extends AnyFunSuiteLike {

  System.setProperty("config.resource", "application.test.conf")
  val pgpassFile = Paths.get(scala.sys.env("HOME"), ".pgpass").toFile
  if (!pgpassFile.exists()) {
    pgpassFile.createNewFile()
  }

  test("testCallbackUrl") {
    var callbackUrl = JobsConfig.createJob.callbackUrl(SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test", Some("test"), Some("path"), Some("url")))
    assert(callbackUrl.isDefined)
    assert(callbackUrl.get == "url")
    callbackUrl = JobsConfig.createJob.callbackUrl(SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test", Some("id")))
    assert(callbackUrl.isDefined)
    assert(callbackUrl.get == "http://django/cohort/cohorts/id/")
    callbackUrl = JobsConfig.createJob.callbackUrl(SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test"))
    assert(callbackUrl.isEmpty)
  }

  test("testRunJob") {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val queryBuilderMock = mock[QueryBuilder]
    val omopTools = mock[OmopTools]
    class JobUtilsMock extends JobUtilsService {
      override def getOmopTools(session: SparkSession, env: JobEnv, stringToString: Map[String, String]): OmopTools = {
        omopTools
      }

      override def getSolrConf(env: JobEnv): Map[String, String] = Map.empty

      override def getRandomIdNotInTabooList(allTabooId: List[Short]): Short = 99
    }

    val createJob = CreateQuery(queryBuilderMock, new JobUtilsMock)

    val unknownResType =
      """
        {"cohortUuid":"ecd89963-ac90-470d-a397-c846882615a6","sourcePopulation":{"caresiteCohortList":[31558]},"_type":"request","resourceType":"foo","request":{"_type":"andGroup","_id":0,"isInclusive":true,"criteria":[{"_type":"basicResource","_id":1,"isInclusive":true,"resourceType":"patientAphp","filterSolr":"fq=gender:f&fq=deceased:false&fq=active:true","filterFhir":"active=true&gender=f&deceased=false&age-day=ge0&age-day=le130"}],"temporalConstraints":[]}}"
      """.stripMargin
    var error = intercept[RuntimeException] {
      createJob.runJob(
        sparkSession,
        JobEnv("someid", AppConfig.get),
        SparkJobParameter(
          "testCohort",
          None,
          unknownResType,
          "someOwnerId"
        )
      )
    }
    assert(error.getMessage == "Resource type not supported")

    val emptyRequest =
      """
      {"cohortUuid":"ecd89963-ac90-470d-a397-c846882615a6","sourcePopulation":{"caresiteCohortList":[31558]},"_type":"request","resourceType":"DocumentReference"}"
    """.stripMargin
    error = intercept[RuntimeException] {
      createJob.runJob(
        sparkSession,
        JobEnv("someid", AppConfig.get),
        SparkJobParameter(
          "testCohort",
          None,
          emptyRequest,
          "someOwnerId"
        )
      )
    }
    assert(error.getMessage == "Request is empty")

    val invalidNonPatientRequest =
      """
      {"cohortUuid":"ecd89963-ac90-470d-a397-c846882615a6","sourcePopulation":{"caresiteCohortList":[31558]},"_type":"request","resourceType":"DocumentReference","request":{"_type":"andGroup","_id":0,"isInclusive":true,"criteria":[{"_type":"basicResource","_id":1,"isInclusive":true,"resourceType":"patientAphp","filterSolr":"fq=gender:f&fq=deceased:false&fq=active:true","filterFhir":"active=true&gender=f&deceased=false&age-day=ge0&age-day=le130"}],"temporalConstraints":[]}}"
    """.stripMargin
    error = intercept[RuntimeException] {
      createJob.runJob(
        sparkSession,
        JobEnv("someid", AppConfig.get),
        SparkJobParameter(
          "testCohort",
          None,
          invalidNonPatientRequest,
          "someOwnerId"
        )
      )
    }
    assert(error.getMessage == "Non-patient resource filter request should be a basic resource")


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
    val request =
      """
      {"cohortUuid":"ecd89963-ac90-470d-a397-c846882615a6","sourcePopulation":{"caresiteCohortList":[31558]},"_type":"request","request":{"_type":"andGroup","_id":0,"isInclusive":true,"criteria":[{"_type":"basicResource","_id":1,"isInclusive":true,"resourceType":"patientAphp","filterSolr":"fq=gender:f&fq=deceased:false&fq=active:true","filterFhir":"active=true&gender=f&deceased=false&age-day=ge0&age-day=le130"}],"temporalConstraints":[]}}"
    """.stripMargin
    val res = createJob.runJob(
      sparkSession,
      JobEnv("someid", AppConfig.get),
      SparkJobParameter(
        "testCohort",
        None,
        request,
        "someOwnerId"
      )
    )
    assert(res.status == "FINISHED")
    assert(res.data("group.count") == "2")
    assert(res.data("group.id") == "0")
  }

}
