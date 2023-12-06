package fr.aphp.id.eds.requester.jobs

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession


case class JobEnv(jobId: String, contextConfig: Config) {
}
case class JobBaseResult(status: String, data: Map[String, String], extra: Map[String, String] = Map.empty)

trait JobBase {
  def runJob(spark: SparkSession, runtime: JobEnv, data: SparkJobParameter): JobBaseResult

  def callbackUrl(jobData: SparkJobParameter): Option[String] = jobData.callbackUrl
}

object JobExecutionStatus {
  type JobExecutionStatus = String
  val PENDING = "PENDING"
  val LONG_PENDING = "LONG_PENDING"
  val STARTED = "STARTED"
  val ERROR = "ERROR"
  val KILLED = "KILLED"
  val FINISHED = "FINISHED"
  val RUNNING = "RUNNING"
  val UNKNOWN = "UNKNOWN"
}
