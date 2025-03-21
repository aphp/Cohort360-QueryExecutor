package fr.aphp.id.eds.requester

import fr.aphp.id.eds.requester.jobs._
import fr.aphp.id.eds.requester.query.engine._
import fr.aphp.id.eds.requester.query.model.{BasicResource, GroupResource}
import fr.aphp.id.eds.requester.query.resolver.ResourceResolver
import fr.aphp.id.eds.requester.tools.JobUtils.initStageDetails
import fr.aphp.id.eds.requester.tools.{JobUtils, JobUtilsService}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

import java.security.SecureRandom

object CountOptions extends Enumeration {
  type CountOptions = String
  val details = "details"
}

object CountOptionsDetails extends Enumeration {
  type CountOptionsDetails = String
  val all = "all"
  val ratio = "ratio"
}

case class CountQuery(queryBuilder: QueryBuilder = new DefaultQueryBuilder(),
                      jobUtilsService: JobUtilsService = JobUtils)
    extends JobBase {

  private val RANGE_MIN = 25
  private val RANGE_MAX = 50
  private val logger = Logger.getLogger(this.getClass)

  override def callbackUrl(jobData: SparkJobParameter): Option[String] = {
    val overrideCallback = super.callbackUrl(jobData)
    if (overrideCallback.isDefined) {
      overrideCallback
    } else if (jobData.cohortUuid.isDefined && AppConfig.get.back.url.isDefined) {
      Some(AppConfig.get.back.url.get + "/cohort/dated-measures/" + jobData.cohortUuid.get + "/")
    } else {
      Option.empty
    }
  }

  override def runJob(spark: SparkSession,
                      runtime: JobEnv,
                      data: SparkJobParameter): JobBaseResult = {
    logger.info("[COUNT] New " + data.mode + " asked by " + data.ownerEntityId)
    val (request, criterionTagsMap, _, resourceResolver, cacheEnabled) =
      jobUtilsService.initSparkJobRequest(logger, spark, runtime, data)
    val stageDetails = initStageDetails(data.modeOptions, request)

    def isGroupResourceAndHasCriteria =
      request.request.get.isInstanceOf[GroupResource] && request.request.get
        .asInstanceOf[GroupResource]
        .criteria
        .nonEmpty

    def isInstanceOfBasicResource = request.request.get.isInstanceOf[BasicResource]

    def theRequestHasAtLeastOneCriteria(): Boolean = {
      request.request.isDefined && (isInstanceOfBasicResource || isGroupResourceAndHasCriteria)
    }

    def processQueryWithSpark(withOrganizationsDetails: Boolean) = {
      val t0 = System.nanoTime()
      val resultDf = queryBuilder
        .processRequest(
          spark,
          request,
          criterionTagsMap,
          stageDetails,
          data.ownerEntityId,
          cacheEnabled,
          withOrganizationsDetails,
          new QueryBuilderGroup(
            new QueryBuilderBasicResource(resourceResolver),
            options = QueryExecutionOptions(resourceResolver.getConfig,
                                            withOrganizations = withOrganizationsDetails),
            jobUtilsService = jobUtilsService
          )
        )
      val t1 = System.nanoTime()
      logger.info("Query Count final dataframe processed in: " + (t1 - t0) / 1000 + "ms")
      resultDf
    }

    def countPatientsWithSpark() = {
      val resultDf = processQueryWithSpark(withOrganizationsDetails = false)
      val count = resultDf.count()
      count
    }

    def countPatientsWithResolver() = {
      ResourceResolver
        .get(data.resolver, data.resolverOpts)
        .countPatients(request.sourcePopulation)
    }

    def countPatientsInQuery() = {
      if (theRequestHasAtLeastOneCriteria()) countPatientsWithSpark()
      else countPatientsWithResolver()
    }

    def computeRatio(criteriaDataframe: DataFrame, resultDataframe: DataFrame, resultCount: Long) = {
      val count = criteriaDataframe.join(resultDataframe, ResultColumn.SUBJECT).count()
      val ratio = count.toDouble / resultCount
      ratio.toString
    }

    if (request.sourcePopulation.cohortList.isEmpty)
      throw new Exception(
        "INPUT JSON cannot be processed (missing input 'sourcePopulation' and/or 'request')")

    val t0 = System.nanoTime()
    val (dataResult, extra: Map[String, String]) = if (data.mode == JobType.countAll) {
      val countResult = countPatientsInQuery()
      (Map("minimum" -> getMinimum(countResult.toInt).toString,
           "maximum" -> getMaximum(countResult.toInt).toString,
           "count" -> countResult.toString),
       Map.empty)

    } else {
      val result = processQueryWithSpark(
        withOrganizationsDetails = data.mode == JobType.countWithDetails)
      if (data.mode == JobType.countWithDetails) {
        val counts = result
        // unless option("flatten_multivalued", "false") is used in solr query
        //.withColumn(ResultColumn.ORGANIZATIONS, functions.split(col(ResultColumn.ORGANIZATIONS), ","))
          .withColumn(ResultColumn.ORGANIZATION, explode(col(ResultColumn.ORGANIZATIONS)))
          .groupBy(ResultColumn.ORGANIZATION)
          .count()
          .collect()
          .foldLeft(Map[String, String]()) {
            case (map, row) => map + (row.getLong(0).toString -> row.getLong(1).toString)
          }
        val total = result.count()
        (Map("count" -> total.toString), counts)
      } else {
        val resultCount = result.count()
        val extraDetails = stageDetails.stageDfs
          .map(
            dfs =>
              dfs.map(x =>
                s"criteria_ratio_${x._1}" -> computeRatio(x._2, result, resultCount))
              .toMap
          )
          .getOrElse(
            stageDetails.stageCounts
              .getOrElse(Map.empty)
              .map(x => s"criteria_count_${x._1}" -> x._2.toString)
              .toMap)
        (Map("count" -> resultCount.toString), extraDetails)
      }
    }
    val t1 = System.nanoTime()
    logger.info("Query Count processed in: " + (t1 - t0) / 1000 + "ms")
    JobBaseResult(JobExecutionStatus.FINISHED, dataResult, extra)
  }

  private def getMaximum(count: Int) =
    new SecureRandom().nextInt(RANGE_MAX - RANGE_MIN) + count + RANGE_MIN

  private def getMinimum(count: Int) = {
    val limMax =
      if (count - RANGE_MIN <= 1) count
      else count - RANGE_MIN
    val lowBound = Math.max(count - RANGE_MAX, 0)
    if (count <= 0) 0
    else new SecureRandom().nextInt(limMax - lowBound) + lowBound
  }
}
