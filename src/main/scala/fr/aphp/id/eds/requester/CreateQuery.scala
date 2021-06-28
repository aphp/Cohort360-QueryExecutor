package fr.aphp.id.eds.requester

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import fr.aphp.id.eds.requester.JobUtils.{initSparkJobRequest, addEmptyGroup}
import fr.aphp.id.eds.requester.jobs.{JobBase, JobEnv}
import fr.aphp.id.eds.requester.query._
import fr.aphp.id.eds.requester.tools.HttpTools.{getBasicBearerTokenHeader, httpPatchRequest}
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions => F}

object CreateQuery extends JobBase {
  type JobData = SparkJobParameter
  type JobOutput = Map[String, String]

  val logger: Logger = Logger.getLogger(this.getClass)

  private val LIMIT = sys.env.getOrElse("COHORT_CREATION_LIMIT", 20000).toString.toInt
  private val djangoUrl = sys.env.getOrElse("DJANGO_CALLBACK_URL", throw new RuntimeException("No Django URL provided"))
  private val token = sys.env.getOrElse("SJS_TOKEN", throw new RuntimeException("No token provided"))

  override def runJob(
      spark: SparkSession,
      runtime: JobEnv,
      data: JobData
  ): JobOutput = {
    implicit val (request, criterionTagsMap, solrConf, omopTools, cacheEnabled) =
      initSparkJobRequest(logger, spark, runtime, data)

    // Init values here because we are in an object (i.e a singleton) and not a class
    var status: String = ""
    var cohortDefinitionId: Long = -1
    var count: Long = -1

    try {
      val isRequestEmpty: Boolean = request.request.isEmpty
      val (completeRequest, completedCriterionTagsMap): (Request, Map[Short, CriterionTags]) =
        if (isRequestEmpty)
          addOneEmptyGroupToRequest(request)
        else (request, criterionTagsMap)

      var cohort = QueryBuilder.processRequest(spark,
                                               solrConf,
                                               completeRequest,
                                               completedCriterionTagsMap,
                                               omopTools,
                                               data.ownerEntityId,
                                               cacheEnabled)

      // get a new cohortId
      cohortDefinitionId = omopTools.getCohortDefinitionId(
        data.cohortDefinitionName,
        data.cohortDefinitionDescription,
        data.cohortDefinitionSyntax,
        data.ownerEntityId
      )

      // filter df columns
      cohort = cohort.select(
        List("subject_id", "encounter", "entryEvent", "exitEvent")
          .filter(c => cohort.columns.contains(c))
          .map(c => F.col(c)): _*)

      cohort.cache()
      count = cohort.dropDuplicates().count()
      val cohortSizeBiggerThanLimit = count > LIMIT

      status = if (cohortSizeBiggerThanLimit) "long_pending" else "finished"

      //  upload into pg and solr
      omopTools.uploadCohort(
        cohortDefinitionId,
        cohort,
        completeRequest.sourcePopulation,
        count,
        cohortSizeBiggerThanLimit
      )

      getCreationResult(cohortDefinitionId, count, status)
    } catch {
      case e: Exception =>
        status = "failed"
        logger.error("Failed with error", e)
        throw e
    } finally {
      val result = getCreationResult(cohortDefinitionId, count, status)
      logger.info(s"Result is ${result}")
      if (djangoUrl.nonEmpty && data.cohortUuid.isDefined) {
        val resultAsJson = new ObjectMapper()
          .registerModule(DefaultScalaModule)
          .writeValueAsString(result)
        httpPatchRequest(djangoUrl + "/cohort/cohorts/" + data.cohortUuid.get + "/", getBasicBearerTokenHeader(token), resultAsJson)
      }
    }
  }

  private def getCreationResult(cohortDefinitionId: Long,
                                count: Long,
                                status: String): JobOutput = {
    Map(
      "group.id" -> cohortDefinitionId.toString,
      "group.count" -> count.toString,
      "request_job_status" -> status
    )
  }

  private def addOneEmptyGroupToRequest(request: Request): (Request, Map[Short, CriterionTags]) = {
    val completeRequest: Request =
      Request(sourcePopulation = request.sourcePopulation, request = Some(addEmptyGroup(List())))
    val completeTagsPerIdMap: Map[Short, CriterionTags] = Map(
      completeRequest.request.get.i -> new CriterionTags(false,
                                                         false,
                                                         false,
                                                         List[String](),
        SolrCollection.PATIENT_APHP,
                                                         List[String]()))
    (completeRequest, completeTagsPerIdMap)
  }

  /** Check that all inputs are defined and have the right format. This method is required by the SJS. */
//  override def validate(
//      sc: SparkSession,
//      runtime: JobEnvironment,
//      config: Config
//  ): JobData Or Every[ValidationProblem] = {
//    try {
//      val res: JobData = SparkJobParameter(
//        cohortDefinitionName = config.getString("input.cohortDefinitionName"),
//        cohortDefinitionDescription =
//          Try(Some(config.getString("input.cohortDefinitionDescription")))
//            .getOrElse(None),
//        cohortDefinitionSyntax = config.getString("input.cohortDefinitionSyntax"),
//        ownerEntityId = config.getString("input.ownerEntityId"),
//        cohortUuid = Some(config.getString("input.cohortUuid"))
//      )
//      Good(res)
//    } catch {
//      case e: Exception =>
//        Bad(One(SingleProblem(e.getMessage)))
//    }
//  }

}
