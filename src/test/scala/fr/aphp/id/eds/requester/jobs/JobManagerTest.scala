package fr.aphp.id.eds.requester.jobs

import com.typesafe.config.ConfigFactory
import fr.aphp.id.eds.requester.SparkJobParameter
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuiteLike

class JobManagerTest extends AnyFunSuiteLike {
  val jobManager = new JobManager(ConfigFactory.load("application.test.conf"))
  val jobStart = new java.util.concurrent.CountDownLatch(1)

  class JobTest extends JobBase {
    override type JobData = SparkJobParameter
    override type JobOutput = String

    override def runJob(spark: SparkSession, runtime: JobEnv, data: SparkJobParameter): String = {
      jobStart.await()
      "ok"
    }
  }

  test("testJobs") {
    val job = jobManager.execJob(new JobTest(), SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test", Some("test")))
    assert(jobManager.list().size == 1)
    assert(jobManager.status(job.jobId).status == JobExecutionStatus.RUNNING)
    jobStart.countDown()
  }

}
