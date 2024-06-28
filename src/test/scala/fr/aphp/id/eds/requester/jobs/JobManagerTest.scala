package fr.aphp.id.eds.requester.jobs

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{
  aResponse,
  getAllServeEvents,
  stubFor,
  urlEqualTo
}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.extension.{Parameters, ServeEventListener}
import com.github.tomakehurst.wiremock.stubbing.ServeEvent
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuiteLike

import java.nio.file.Paths

class JobManagerTest extends AnyFunSuiteLike with BeforeAndAfterEach {
  System.setProperty("config.resource", "application.test.conf")
  if (!Paths.get("solr_auth.txt").toFile.exists()) {
    Paths.get("solr_auth.txt").toFile.createNewFile()
  }
  val jobManager = new JobManager()
  val jobStart = new java.util.concurrent.CountDownLatch(1)
  val jobEnd = new java.util.concurrent.CountDownLatch(1)

  val Port = 8080
  val Host = "localhost"
  val wireMockServer = new WireMockServer(
    wireMockConfig().extensions(new MyServeEventListener()).port(Port))

  class MyServeEventListener extends ServeEventListener {

    override def getName: String = "???"

    override def afterComplete(serveEvent: ServeEvent, parameters: Parameters): Unit = {
      jobEnd.countDown()
    }
  }

  override def beforeEach {
    wireMockServer.start()
  }

  override def afterEach {
    wireMockServer.stop()
  }

  class JobTest extends JobBase {
    override def runJob(spark: SparkSession,
                        runtime: JobEnv,
                        data: SparkJobParameter): JobBaseResult = {
      jobStart.await()
      JobBaseResult(JobExecutionStatus.FINISHED, Map("ok" -> "ok"))
    }
  }

  def testJob(jobObject: JobBase,
              jobParam: SparkJobParameter,
              result: JobResult,
              callbackResult: String): Unit = {
    val job = jobManager.execJob(jobObject, jobParam)
    assert(jobManager.list().size == 1)
    assert(jobManager.status(job.jobId).status == JobExecutionStatus.RUNNING)

    stubFor(
      WireMock
        .patch(urlEqualTo("/"))
        .willReturn(aResponse()
          .withStatus(200)))

    jobStart.countDown()
    jobEnd.await()
    assert(jobManager.status(job.jobId).status == JobExecutionStatus.FINISHED)
    assert(jobManager.status(job.jobId).result == List(result))
    val allServeEvents = getAllServeEvents()
    assert(allServeEvents.size() == 1)
    assert(allServeEvents.get(0).getRequest().getBodyAsString() == callbackResult)
  }

  // TODO test the message sent to wiremock and also test with real job types to see if the results are properly built
  test("testJobsTypes") {
    testJob(
      new JobTest(),
      SparkJobParameter("test",
                        Some("test"),
                        "test",
                        "test",
                        "test",
                        "test",
                        "test",
                        Some("test"),
                        callbackUrl = Some(wireMockServer.baseUrl())),
      JobResult("test", JobBaseResult(JobExecutionStatus.FINISHED, Map("ok" -> "ok")).toString),
      "{\"request_job_status\":\"FINISHED\",\"ok\":\"ok\",\"extra\":{}}"
    )
  }

}
