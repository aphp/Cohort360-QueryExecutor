package fr.aphp.id.eds.requester.jobs

import com.typesafe.config.{Config, ConfigFactory}
import fr.aphp.id.eds.requester.tools.HttpTools.httpPatchRequest
import fr.aphp.id.eds.requester.{CountQuery, CreateQuery, SparkJobParameter}
import org.apache.http.HttpException
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.time.{OffsetDateTime, ZoneId}
import java.util.UUID
import java.util.concurrent.Executors
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}



case class JobInfo(status: String,
                   jobId: String,
                   context: String,
                   startTime: OffsetDateTime,
                   duration: String,
                   result: Option[AnyRef],
                   classPath: String,
                   execution: Future[AnyRef])

class JobManager() {
  val conf: Config = ConfigFactory.load
  val sparkSession: SparkSession = SparkConfig.sparkSession
  implicit val ec: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(conf.getInt("app.jobs.threads")))
  val jobs: mutable.Map[String, JobInfo] = TrieMap()
  private val logger = Logger.getLogger(this.getClass)

  sparkSession.sparkContext.addFile("solr_auth.txt")

  def execJob(jobExecutor: JobBase, jobData: SparkJobParameter): JobStatus = {
    val jobId = UUID.randomUUID().toString
    logger.info(s"Starting new job ${jobId}")
    val jobExec = Future {
      logger.info(s"Job ${jobId} started")
      sparkSession.sparkContext.setJobGroup(jobId, s"new job ${jobId}", interruptOnCancel = true)
      sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
      jobExecutor.runJob(sparkSession, JobEnv(jobId, conf), jobData)
    }
    jobs(jobId) = JobInfo(
      JobExecutionStatus.RUNNING,
      jobId,
      "",
      OffsetDateTime.now(ZoneId.of("UTC")),
      "",
      Option.empty,
      jobExecutor.getClass.getCanonicalName,
      jobExec
    )
    val job = jobs(jobId)
    jobExec.andThen {
      case Success(result) =>
        logger.info(s"Job ${jobId} successfully executed")
        finalizeJob(jobId, Right(result), jobExecutor, jobData.mode, jobData)
      case Failure(wrapped: Throwable) =>
        logger.error(s"Job ${jobId} failed", wrapped)
        finalizeJob(jobId, Left(wrapped), jobExecutor, jobData.mode, jobData)
    }
    JobStatus(job.status,
              job.jobId,
              job.context,
              job.startTime.toString,
              job.duration,
              job.result.getOrElse(""),
              job.classPath)
  }

  private def finalizeJob(jobId: String,
                          result: Either[Throwable, Map[String, String]],
                          jobExecutor: JobBase,
                          jobMode: String,
                          callbackUrl: SparkJobParameter): Unit = {
    updateJob(jobId, result, jobExecutor, jobMode)
    callCallback(jobExecutor, callbackUrl, jobId, result)
  }

  private def callCallback(jobExecutor: JobBase,
                           jobData: SparkJobParameter,
                           jobId: String,
                           result: Either[Throwable, Map[String, String]]): Unit = {
    val callBackUrlOpt = jobExecutor.callbackUrl(jobData)
    if (callBackUrlOpt.isDefined) {
      val callback = callBackUrlOpt.get
      logger.info(s"Calling callback at ${callback} for job ${jobId}")
      val callbackResult = result match {
        case Left(_) => Map("status" ->  JobExecutionStatus.ERROR)
        case Right(value) => {
          value
        }
      }
      try {
        httpPatchRequest(callback, callbackResult)
      } catch {
        case e: HttpException => {
          logger.error(s"Failed to call callback ${callback} for job ${jobId}", e)
        }
      }
    }
  }

  private def updateJob(jobId: String,
                        result: Either[Throwable, Map[String, String]],
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

  private def buildResult(result: Either[Throwable, Map[String, String]],
                          jobMode: String,
                          jobExecutor: JobBase): JobResult = {
    val jobModeStr = jobMode.replace("count_all", "countAll")
    result match {
      case Left(wrappedError) => JobResult(jobModeStr, wrappedError.getMessage)
      case Right(value) =>
        jobExecutor match {
          case CountQuery =>
            val count = try {
              value("count").toLong
            } catch {
              case e: NumberFormatException => {
                logger.warn("Failed to parse count query result count", e)
                -1
              }
            }
            JobResult(jobModeStr, "", "", count)
          case CreateQuery => {
            val resMap = value
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
