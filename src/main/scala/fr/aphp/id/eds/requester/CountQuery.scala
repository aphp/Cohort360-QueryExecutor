package fr.aphp.id.eds.requester

import fr.aphp.id.eds.requester.jobs.{JobBase, JobEnv, JobExecutionStatus, SparkJobParameter}
import fr.aphp.id.eds.requester.query.{BasicResource, GroupResource, QueryBuilder}
import fr.aphp.id.eds.requester.tools.JobUtils.getDefaultSolrFilterQuery
import fr.aphp.id.eds.requester.tools.SolrTools.getSolrClient
import fr.aphp.id.eds.requester.tools.{JobUtils, JobUtilsService}
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.sql.SparkSession

import java.security.SecureRandom

case class CountQuery(queryBuilder: QueryBuilder = QueryBuilder, jobUtilsService: JobUtilsService = JobUtils) extends JobBase {

  private val RANGE_MIN = 25
  private val RANGE_MAX = 50
  private val logger = Logger.getLogger(this.getClass)

  override def callbackUrl(jobData: SparkJobParameter): Option[String] = {
    val overrideCallback = super.callbackUrl(jobData)
    if (overrideCallback.isDefined) {
      overrideCallback
    } else if (jobData.cohortUuid.isDefined) {
      Some(AppConfig.djangoUrl + "/cohort/dated-measures/" + jobData.cohortUuid.get + "/")
    } else {
      Option.empty
    }
  }
  override def runJob(spark: SparkSession, runtime: JobEnv, data: SparkJobParameter): Map[String, String] = {
    logger.info("[COUNT] New " + data.mode + " asked by " + data.ownerEntityId)
    val (request, criterionTagsMap, solrConf, omopTools, cacheEnabled) =
      jobUtilsService.initSparkJobRequest(logger, spark, runtime, data)

    def isGroupResourceAndHasCriteria =
      request.request.get.isInstanceOf[GroupResource] && request.request.get
        .asInstanceOf[GroupResource]
        .criteria
        .nonEmpty

    def isInstanceOfBasicResource = request.request.get.isInstanceOf[BasicResource]

    def theRequestHasAtLeastOneCriteria(): Boolean = {
      request.request.isDefined && (isInstanceOfBasicResource || isGroupResourceAndHasCriteria)
    }

    def countPatientsWithSpark() = {
      queryBuilder
        .processRequest(spark,
                        solrConf,
                        request,
                        criterionTagsMap,
                        omopTools,
                        data.ownerEntityId,
                        cacheEnabled)
        .count()
    }

    def countPatientsInSolr() = {
      val solr = getSolrClient(solrConf("zkhost"))
      val query =
        new SolrQuery("*:*").addFilterQuery(getDefaultSolrFilterQuery(request.sourcePopulation))
      val res = solr.query(SolrCollection.PATIENT_APHP, query)
      solr.close()
      res.getResults.getNumFound
    }

    def countPatientsInQuery() = {
      if (theRequestHasAtLeastOneCriteria()) countPatientsWithSpark() else countPatientsInSolr()
    }

    if (request.sourcePopulation.caresiteCohortList.isEmpty)
      throw new Exception(
        "INPUT JSON cannot be processed (missing input 'sourcePopulation' and/or 'request')")

    val countResult = countPatientsInQuery()
    if (data.mode == "count_all") {
      Map("request_job_status" -> JobExecutionStatus.FINISHED, "minimum" -> getMinimum(countResult.toInt).toString, "maximum" -> getMaximum(countResult.toInt).toString, "count" -> countResult.toString)
    } else {
      Map("request_job_status" -> JobExecutionStatus.FINISHED, "count" -> countResult.toString)
    }
  }

  private def getMaximum(count: Int) = new SecureRandom().nextInt(RANGE_MAX - RANGE_MIN) + count + RANGE_MIN

  private def getMinimum(count: Int) = {
    val limMax = if (count - RANGE_MIN <= 1) count
    else count - RANGE_MIN
    val lowBound = Math.max(count - RANGE_MAX, 0)
    if (count <= 0) 0
    else new SecureRandom().nextInt(limMax - lowBound) + lowBound
  }

//  /** Check that all inputs are defined and have the right format. This method is required by the SJS. */
//  override def validate(sc: SparkSession, runtime: JobEnvironment, config: Config): JobData Or Every[ValidationProblem] = {
//    try {
//      val res: JobData = SparkJobParameter(
//        cohortDefinitionName = config.getString("input.cohortDefinitionName"),
//        cohortDefinitionDescription =
//          Try(Some(config.getString("input.cohortDefinitionDescription")))
//            .getOrElse(None),
//        cohortDefinitionSyntax = config.getString("input.cohortDefinitionSyntax"),
//        ownerEntityId = config.getString("input.ownerEntityId"),
//        mode = config.getString("input.mode"),
//        cohortUuid = None
//      )
//      Good(res)
//    } catch {
//      case e: Exception => Bad(One(SingleProblem(e.getMessage)))
//    }
//  }
}
