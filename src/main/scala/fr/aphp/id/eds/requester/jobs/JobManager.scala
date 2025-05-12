package fr.aphp.id.eds.requester.jobs

import fr.aphp.id.eds.requester.tools.HttpTools.httpPatchRequest
import fr.aphp.id.eds.requester.{AppConfig, CountQuery, CreateQuery}
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

class JobManager(val sparkSession: SparkSession = SparkConfig.sparkSession) {
  implicit val ec: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(AppConfig.get.business.jobs.threads))
  val jobs: mutable.Map[String, JobInfo] = TrieMap()
  private val logger = Logger.getLogger(this.getClass)

  if (AppConfig.get.solr.isDefined) {
    sparkSession.sparkContext.addFile(AppConfig.get.solr.get.authFile)
  }

  def execJob(jobExecutor: JobBase, jobData: SparkJobParameter, retry: Int = 0): JobStatus = {
    val jobId = UUID.randomUUID().toString
    logger.info(s"Starting new job ${jobId}")
    val autoRetry = AppConfig.get.business.jobs.autoRetry
    val jobExec = Future {
      logger.info(s"Job ${jobId} started")
      sparkSession.sparkContext.setJobGroup(jobId, s"new job ${jobId}", interruptOnCancel = true)
      sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
      jobExecutor.runJob(sparkSession, JobEnv(jobId, AppConfig.get), jobData)
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
        if (retry < autoRetry && !isACancellationError(wrapped)) {
          logger.info(s"Retrying job ${jobId}")
          updateJob(jobId, Left(wrapped), jobExecutor, jobData.mode)
          execJob(jobExecutor, jobData, retry + 1)
        } else {
          finalizeJob(jobId, Left(wrapped), jobExecutor, jobData.mode, jobData)
        }
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
                          result: Either[Throwable, JobBaseResult],
                          jobExecutor: JobBase,
                          jobMode: String,
                          callbackUrl: SparkJobParameter): Unit = {
    updateJob(jobId, result, jobExecutor, jobMode)
    callCallback(jobExecutor, callbackUrl, jobId, result)
  }

  private def callCallback(jobExecutor: JobBase,
                           jobData: SparkJobParameter,
                           jobId: String,
                           result: Either[Throwable, JobBaseResult]): Unit = {
    val callbackResult = result match {
      case Left(wrapped) =>
        Map("request_job_status" -> jobs(jobId).status,
            "message" -> wrapped.getMessage.replaceAll("[^\\x20-\\x7E]", ""))
      case Right(value) => {
        Map("request_job_status" -> value.status) ++ value.data ++ Map("extra" -> value.extra)
      }
    }

    val callBackUrlOpt = jobExecutor.callbackUrl(jobData)
    if (callBackUrlOpt.isDefined) {
      val callback = callBackUrlOpt.get
      logger.info(s"Calling callback at ${callback} for job ${jobId}")
      try {
        httpPatchRequest(callback, callbackResult)
      } catch {
        case e: HttpException => {
          logger.error(s"Failed to call callback ${callback} for job ${jobId}", e)
        }
      }
    } else {
      logger.warn(s"No callback defined for job ${jobId}")
      logger.info("Callback result: " + callbackResult)
    }
  }

  private def updateJob(jobId: String,
                        result: Either[Throwable, JobBaseResult],
                        jobExecutor: JobBase,
                        jobMode: String): Unit = {
    val updatedJob = jobs(jobId)
    val duration =
      s"${OffsetDateTime.now(ZoneId.of("UTC")).toInstant.toEpochMilli - updatedJob.startTime.toInstant.toEpochMilli} ms"
    val formattedResult = buildResult(result, jobMode, jobExecutor)
    val (status, execution) =
      result match {
        case Left(wrappedError) => {
          if (isACancellationError(wrappedError)) {
            (JobExecutionStatus.KILLED, Future.failed(wrappedError.getCause))
          } else {
            (JobExecutionStatus.ERROR, Future.failed(wrappedError.getCause))
          }
        }
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

  private def isACancellationError(wrappedError: Throwable): Boolean = {
    wrappedError.getMessage.contains("cancelled part of cancelled job group")
  }

  private def buildResult(result: Either[Throwable, JobBaseResult],
                          jobMode: String,
                          jobExecutor: JobBase): JobResult = {
    val jobModeStr = jobMode.replace("count_all", "countAll")
    result match {
      case Left(wrappedError) => JobResult(jobModeStr, wrappedError.getMessage)
      case Right(value) =>
        jobExecutor match {
          case _: CountQuery =>
            val count = try {
              value.data("count").toLong
            } catch {
              case e: NumberFormatException => {
                logger.warn("Failed to parse count query result count", e)
                -1
              }
            }
            JobResult(jobModeStr, value.status, "", count, extra = value.extra)
          case _: CreateQuery => {
            val resMap = value
            val count = try {
              resMap.data("group.count").toLong
            } catch {
              case e: NumberFormatException => {
                logger.warn("Failed to parse create query result group count", e)
                -1
              }
            }
            JobResult(jobModeStr, resMap.status, resMap.data("group.id"), count)
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

  def cancelJob(jobId: String): JobStatus = {
    logger.info(s"Canceling job ${jobId}")
    sparkSession.sparkContext.cancelJobGroup(jobId)
    val existingJob = jobs(jobId)
    jobs(jobId) = JobInfo(JobExecutionStatus.KILLED,
      jobId,
      "",
      existingJob.startTime,
      s"${OffsetDateTime.now(ZoneId.of("UTC")).toInstant.toEpochMilli - existingJob.startTime.toInstant.toEpochMilli} ms",
      None,
      existingJob.classPath,
      existingJob.execution)
    JobStatus(JobExecutionStatus.KILLED,
      jobId,
      "",
      "",
      "",
      null,
      "")
  }

}
