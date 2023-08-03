package fr.aphp.id.eds.requester.server

import com.typesafe.config.ConfigFactory
import fr.aphp.id.eds.requester.jobs.{JobManager, JobResultSerializer}
import fr.aphp.id.eds.requester.{CountQuery, CreateQuery, SparkJobParameter}
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.ScalatraServlet
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.swagger.{Swagger, SwaggerSupport}


class JobController(implicit val swagger: Swagger) extends ScalatraServlet with SwaggerSupport with JacksonJsonSupport {
  protected val applicationDescription = "The count controller job api"

  // Sets up automatic case class to JSON output serialization
  protected implicit lazy val jsonFormats: Formats = DefaultFormats + JobResultSerializer.serializer

  private val jobManager = new JobManager()

  before() {
    contentType = formats("json")
  }

  private def parseInput(body: String): SparkJobParameter = {
    val configData = ConfigFactory.parseString(body).getConfig("input")
    SparkJobParameter(
      configData.getString("cohortDefinitionName"),
      if (configData.hasPath("cohortDefinitionDescription")) Option(configData.getString("cohortDefinitionDescription")) else Option.empty,
      configData.getString("cohortDefinitionSyntax"),
      configData.getString("ownerEntityId"),
      "10000",
      "10000",
      configData.getString("mode"),
      if (configData.hasPath("cohortUuid")) Option(configData.getString("cohortUuid")) else Option.empty,
      if (configData.hasPath("callbackUrl")) Option(configData.getString("callbackUrl")) else Option.empty
    )
  }

  post("/") {
    val jobData = parseInput(request.body)

    jobData.mode match {
      case "count" => jobManager.execJob(CountQuery, jobData)
      case "count_all" => jobManager.execJob(CountQuery, jobData)
      case "create" => jobManager.execJob(CreateQuery, jobData)
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