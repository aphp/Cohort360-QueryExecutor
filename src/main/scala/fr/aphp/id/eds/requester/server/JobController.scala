package fr.aphp.id.eds.requester.server

import com.typesafe.config.ConfigFactory
import fr.aphp.id.eds.requester.AppConfig
import fr.aphp.id.eds.requester.jobs._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.ScalatraServlet
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.swagger.{Swagger, SwaggerSupport}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class JobController(implicit val swagger: Swagger)
    extends ScalatraServlet
    with SwaggerSupport
    with JacksonJsonSupport {
  protected val applicationDescription = "The count controller job api"

  // Sets up automatic case class to JSON output serialization
  protected implicit lazy val jsonFormats: Formats = DefaultFormats + JobResultSerializer.serializer

  private val jobManager = new JobManager()

  before() {
    contentType = formats("json")
  }

  def parseInput(body: String): SparkJobParameter = {
    val configData = ConfigFactory.parseString(body).getConfig("input")
    SparkJobParameter(
      if (configData.hasPath("cohortDefinitionName")) configData.getString("cohortDefinitionName")
      else "Unnamed Cohort",
      if (configData.hasPath("cohortDefinitionDescription"))
        Option(configData.getString("cohortDefinitionDescription"))
      else Option.empty,
      configData.getString("cohortDefinitionSyntax"),
      if (configData.hasPath("ownerEntityId")) configData.getString("ownerEntityId") else "0",
      "10000",
      "10000",
      configData.getString("mode"),
      if (configData.hasPath("modeOptions"))
        configData
          .getConfig("modeOptions")
          .entrySet()
          .toList
          .map(entry => entry.getKey -> entry.getValue.unwrapped().toString)
          .toMap
      else Map.empty,
      if (configData.hasPath("cohortUuid")) Option(configData.getString("cohortUuid"))
      else Option.empty,
      if (configData.hasPath("existingCohortId")) Option(configData.getLong("existingCohortId"))
      else Option.empty,
      if (configData.hasPath("callbackPath")) Option(configData.getString("callbackPath"))
      else Option.empty,
      if (configData.hasPath("callbackUrl")) Option(configData.getString("callbackUrl"))
      else Option.empty,
      if (configData.hasPath("resolver")) configData.getString("resolver")
      else AppConfig.get.defaultResolver,
      if (configData.hasPath("resolverOpts"))
        configData
          .getConfig("resolverOpts")
          .entrySet()
          .toList
          .map(entry => entry.getKey -> entry.getValue.unwrapped().toString)
          .toMap
      else Map.empty,
      if (configData.hasPath("cohortCreationService"))
        configData.getString("cohortCreationService")
      else AppConfig.get.defaultCohortCreationService
    )
  }

  post("/") {
    val jobData = parseInput(request.body)

    jobData.mode match {
      case JobType.count            => jobManager.execJob(JobsConfig.countJob, jobData)
      case JobType.countAll         => jobManager.execJob(JobsConfig.countJob, jobData)
      case JobType.countWithDetails => jobManager.execJob(JobsConfig.countJob, jobData)
      case JobType.create           => jobManager.execJob(JobsConfig.createJob, jobData)
    }
  }

  get("/") {
    jobManager.list()
  }

  get("/:jobId") {
    val jobId = params("jobId")
    jobManager.status(jobId)
  }

  delete("/:jobId") {
    val jobId = params("jobId")
    jobManager.cancelJob(jobId)
  }

}
