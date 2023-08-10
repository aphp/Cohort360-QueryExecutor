package fr.aphp.id.eds.requester.jobs

import com.typesafe.config.ConfigFactory
import fr.aphp.id.eds.requester.SparkJobParameter
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuiteLike

class JobManagerTest extends AnyFunSuiteLike {
  System.setProperty("config.resource", "application.test.conf")
  val jobManager = new JobManager()
  val jobStart = new java.util.concurrent.CountDownLatch(1)

  class JobTest extends JobBase {
    override type JobData = SparkJobParameter

    override def runJob(spark: SparkSession, runtime: JobEnv, data: SparkJobParameter): Map[String, String] = {
      jobStart.await()
      Map("ok"-> "ok")
    }
  }

  test("testJobs") {
    val job = jobManager.execJob(new JobTest(), SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test", Some("test")))
    assert(jobManager.list().size == 1)
    assert(jobManager.status(job.jobId).status == JobExecutionStatus.RUNNING)
    jobStart.countDown()
  }

}
