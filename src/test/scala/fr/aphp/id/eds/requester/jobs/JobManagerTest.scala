package fr.aphp.id.eds.requester.jobs

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{
  aResponse,
  getAllServeEvents,
  removeServeEvents,
  stubFor,
  urlEqualTo
}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.extension.{Parameters, ServeEventListener}
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder
import com.github.tomakehurst.wiremock.stubbing.ServeEvent
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuiteLike

import java.nio.file.Paths
import java.util.concurrent.CountDownLatch

class JobManagerTest extends AnyFunSuiteLike with BeforeAndAfterEach with BeforeAndAfterAll {
  System.setProperty("config.resource", "application.test.conf")
  if (!Paths.get("solr_auth.txt").toFile.exists()) {
    Paths.get("solr_auth.txt").toFile.createNewFile()
  }
  var sparkSession: SparkSession = _
  var jobManager: JobManager = _

  val serverListener = new MyServeEventListener()
  val Port = 8080
  val Host = "localhost"
  val wireMockServer = new WireMockServer(wireMockConfig().extensions(serverListener).port(Port))

  class MyServeEventListener extends ServeEventListener {
    var jobCounter: CountDownLatch = new CountDownLatch(1)

    def resetCounter(): Unit = {
      jobCounter = new CountDownLatch(1)
    }

    override def getName: String = "testlistener"

    override def afterComplete(serveEvent: ServeEvent, parameters: Parameters): Unit = {
      jobCounter.countDown()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.session.timeZone", "Europe/Paris")
      .getOrCreate()
    jobManager = new JobManager(sparkSession)
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
    wireMockServer.stop()
    super.afterAll()
  }

  override def beforeEach {
    wireMockServer.start()

  }

  override def afterEach {
    wireMockServer.stop()
  }

  class JobTest(val resultGenerator: () => JobBaseResult) extends JobBase {
    val jobStart = new CountDownLatch(1)

    override def runJob(spark: SparkSession,
                        runtime: JobEnv,
                        data: SparkJobParameter): JobBaseResult = {
      jobStart.await()
      resultGenerator()
    }
  }

  def testJob(jobObject: JobTest,
              jobParam: SparkJobParameter,
              result: JobResult,
              callbackResult: String): Unit = {
    val job = jobManager.execJob(jobObject, jobParam)
    assert(jobManager.status(job.jobId).status == JobExecutionStatus.RUNNING)

    stubFor(
      WireMock
        .patch(urlEqualTo("/"))
        .willReturn(aResponse()
          .withStatus(200)))

    jobObject.jobStart.countDown()
    serverListener.jobCounter.await()
    assert(jobManager.status(job.jobId).result == List(result))
    val allServeEvents = getAllServeEvents()
    assert(allServeEvents.size() == 1)
    assert(allServeEvents.get(0).getRequest().getBodyAsString() == callbackResult)
  }

  // TODO test the message sent to wiremock and also test with real job types to see if the results are properly built
  test("testJobsTypes") {
    testJob(
      new JobTest(() => JobBaseResult(JobExecutionStatus.FINISHED, Map("ok" -> "ok"))),
      SparkJobParameter("test",
                        Some("test"),
                        "test",
                        "test",
                        "test",
                        "test",
                        "test",
                        Map.empty,
                        Some("test"),
                        callbackUrl = Some(wireMockServer.baseUrl())),
      JobResult("test", JobBaseResult(JobExecutionStatus.FINISHED, Map("ok" -> "ok")).toString),
      "{\"request_job_status\":\"FINISHED\",\"ok\":\"ok\",\"extra\":{}}"
    )
    assert(jobManager.list().size == 1)
    serverListener.resetCounter()
    removeServeEvents(new RequestPatternBuilder().withUrl("/"))
    testJob(
      new JobTest(() => throw new RuntimeException("error")),
      SparkJobParameter("test",
                        Some("test"),
                        "test",
                        "test",
                        "test",
                        "test",
                        "test",
                        Map.empty,
                        Some("test"),
                        callbackUrl = Some(wireMockServer.baseUrl())),
      JobResult("test", "error"),
      "{\"request_job_status\":\"ERROR\",\"message\":\"error\"}"
    )
    assert(jobManager.list().size == 3) // 2 jobs + 1 error
    val jobId = jobManager.list()(2).jobId
    jobManager.cancelJob(jobId)
    assert(jobManager.status(jobId).status == JobExecutionStatus.KILLED)
  }

}
