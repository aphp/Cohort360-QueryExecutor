package fr.aphp.id.eds.requester.jobs

import com.typesafe.config.{Config, ConfigFactory}
import fr.aphp.id.eds.requester.{CountQuery, CreateQuery, SparkJobParameter}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.time.{OffsetDateTime, ZoneId}
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.{Future, future}
import scala.util.{Failure, Success}

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

case class JobInfo(status: String,
                   jobId: String,
                   context: String,
                   startTime: OffsetDateTime,
                   duration: String,
                   result: Option[AnyRef],
                   classPath: String,
                   execution: Future[AnyRef])

class JobManager(val conf: Config = ConfigFactory.load) {
  val sparkSession: SparkSession = SparkConfig.sparkSession
  val jobs: mutable.Map[String, JobInfo] = TrieMap()
  private val jobConf = conf.getConfig("spark").getConfig("context-settings")
  private val logger = Logger.getLogger(this.getClass)

  def execJob(jobExecutor: JobBase, jobData: SparkJobParameter): JobStatus = {
    val jobId = UUID.randomUUID().toString
    logger.info(s"Starting new job ${jobId}")
    jobs(jobId) = JobInfo(
      JobExecutionStatus.RUNNING,
      jobId,
      "",
      OffsetDateTime.now(ZoneId.of("UTC")),
      "",
      Option.empty,
      jobExecutor.getClass.getCanonicalName,
      Future {
        sparkSession.sparkContext.setJobGroup(jobId, s"new job ${jobId}", true)
        jobExecutor.runJob(sparkSession, JobEnv(jobId, jobConf), jobData).asInstanceOf[AnyRef]
      }.andThen {
        case Success(result) =>
          logger.info(s"Job ${jobId} successfully executed")
          updateJob(jobId, Right(result), jobExecutor, jobData.mode)
        case Failure(wrapped: Throwable) =>
          logger.error(s"Job ${jobId} failed", wrapped.getCause)
          updateJob(jobId, Left(wrapped), jobExecutor, jobData.mode)
      }
    )
    val job = jobs(jobId)
    JobStatus(job.status,
              job.jobId,
              job.context,
              job.startTime.toString,
              job.duration,
              job.result.getOrElse(""),
              job.classPath)
  }

  private def updateJob(jobId: String,
                        result: Either[Throwable, AnyRef],
                        jobExecutor: JobBase,
                        jobMode: String): Unit = {
    val updatedJob = jobs(jobId)
    val duration =
      s"${OffsetDateTime.now(ZoneId.of("UTC")).toInstant.toEpochMilli - updatedJob.startTime.toInstant.toEpochMilli} ms"
    val formattedResult = buildResult(result, jobMode, jobExecutor)
    val (status, execution) =
      result match {
        case Left(wrappedError) => (JobExecutionStatus.ERROR, Future.failed(wrappedError.getCause))
        case Right(value)       => (JobExecutionStatus.FINISHED, Future.successful(value))
      }
    jobs(jobId) = JobInfo(status,
                          jobId,
                          "",
                          updatedJob.startTime,
                          duration,
                          Some(List(formattedResult)),
                          jobExecutor.getClass.getCanonicalName,
                          execution)
  }

  private def buildResult(result: Either[Throwable, AnyRef],
                          jobMode: String,
                          jobExecutor: JobBase): JobResult = {
    val jobModeStr = jobMode.replace("count_all", "countAll")
    result match {
      case Left(wrappedError) => JobResult(jobModeStr, wrappedError.getMessage)
      case Right(value) =>
        jobExecutor match {
          case CountQuery =>
            JobResult(jobModeStr, "", "", value.asInstanceOf[Long])
          case CreateQuery => {
            val resMap = value.asInstanceOf[Map[String, String]]
            val count = try {
              resMap("group.count").toLong
            } catch {
              case e: NumberFormatException => {
                logger.warn("Failed to parse create query result group count", e)
                -1
              }
            }
            JobResult(jobModeStr, resMap("request_job_status"), resMap("group.id"), count)
          }
          case _ => JobResult(jobModeStr, result.right.get.toString)
        }
    }
  }

  def list(): List[JobStatus] = {
    jobs.values
      .map(
        job =>
          JobStatus(job.status,
                    job.jobId,
                    job.context,
                    job.startTime.toString,
                    job.duration,
                    job.result.orNull,
                    job.classPath))
      .toList
  }

  def status(jobId: String): JobStatus = {
    val job = jobs(jobId)
    JobStatus(job.status,
              job.jobId,
              job.context,
              job.startTime.toString,
              job.duration,
              job.result.orNull,
              job.classPath)
  }

  def cancelJob(jobId: String): Unit = {
    logger.info(s"Canceling job ${jobId}")
    sparkSession.sparkContext.cancelJobGroup(jobId)
  }

}
