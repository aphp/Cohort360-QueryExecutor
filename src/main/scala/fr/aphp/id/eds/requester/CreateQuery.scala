package fr.aphp.id.eds.requester

import fr.aphp.id.eds.requester.jobs._
import fr.aphp.id.eds.requester.query.engine._
import fr.aphp.id.eds.requester.query.model.{BasicResource, Request}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.tools.JobUtils.addEmptyGroup
import fr.aphp.id.eds.requester.tools.{JobUtils, JobUtilsService, StageDetails}
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions => F}
import org.hl7.fhir.r4.model.ListResource.ListMode

object CreateOptions extends Enumeration {
  type CreateOptions = String
  val sampling = "sampling"
}

object CreateDiffOptions extends Enumeration {
  type CreateDiffOptions = String
  val baseCohortId = "baseCohortId"
}

case class CreateQuery(queryBuilder: QueryBuilder = new DefaultQueryBuilder(),
                       jobUtilsService: JobUtilsService = JobUtils)
    extends JobBase {
  private val logger: Logger = Logger.getLogger(this.getClass)
  private val LIMIT = AppConfig.get.business.cohortCreationLimit

  override def callbackUrl(jobData: SparkJobParameter): Option[String] = {
    val overrideCallback = super.callbackUrl(jobData)
    if (overrideCallback.isDefined) {
      overrideCallback
    } else if (jobData.cohortUuid.isDefined && AppConfig.get.back.url.isDefined) {
      Some(AppConfig.get.back.url.get + "/cohort/cohorts/" + jobData.cohortUuid.get + "/")
    } else {
      Option.empty
    }
  }

  override def runJob(
      spark: SparkSession,
      runtime: JobEnv,
      data: SparkJobParameter
  ): JobBaseResult = {
    implicit val (request, criterionTagsMap, omopTools, resourceResolver, cacheEnabled) = {
      jobUtilsService.initSparkJobRequest(logger, spark, runtime, data)
    }
    implicit val sparkSession: SparkSession = spark

    validateRequestOrThrow(request)
    validateModeOptionsOrThrow(data)

    // Init values here because we are in an object (i.e a singleton) and not a class
    var status: String = ""
    var cohortDefinitionId: Long = -1
    var count: Long = -1

    val isRequestEmpty: Boolean = request.request.isEmpty
    val (completeRequest, completedCriterionTagsMap): (Request, Map[Short, CriterionTags]) =
      if (isRequestEmpty)
        addOneEmptyGroupToRequest(request)
      else (request, criterionTagsMap)

    var cohort = queryBuilder.processRequest(
      spark,
      completeRequest,
      completedCriterionTagsMap,
      StageDetails(None, None),
      data.ownerEntityId,
      cacheEnabled,
      withOrganizationDetails = false,
      new QueryBuilderGroup(new QueryBuilderBasicResource(resourceResolver),
                            options = QueryExecutionOptions(resourceResolver.getConfig),
                            jobUtilsService = jobUtilsService)
    )

    // filter df columns
    cohort = cohort
      .select(
        List(ResultColumn.SUBJECT, "encounter", "entryEvent", "exitEvent")
          .filter(c => cohort.columns.contains(c))
          .map(c => F.col(c)): _*)
      .dropDuplicates()
      .withColumn("deleted", F.lit(false))

    if (data.modeOptions.contains(CreateOptions.sampling)) {
      val sampling = data.modeOptions(CreateOptions.sampling).toDouble
      // https://stackoverflow.com/questions/37416825/dataframe-sample-in-apache-spark-scala#comment62349780_37418684
      // to be sure to have the right number of rows
      cohort = cohort.sample(sampling + 0.1).limit((sampling * cohort.count()).round.toInt)
    }
    cohort.cache()
    count = cohort.count()
    val cohortSizeBiggerThanLimit = count > LIMIT

    def createNewCohort(): Long = {
      omopTools
        .map(
          t =>
            t.createCohort(
              data.cohortDefinitionName,
              data.cohortDefinitionDescription,
              data.cohortDefinitionSyntax,
              data.ownerEntityId,
              request.resourceType,
              if (data.mode == JobType.createDiff && data.modeOptions.contains(
                    CreateDiffOptions.baseCohortId))
                Some(data.modeOptions(CreateDiffOptions.baseCohortId).toLong)
              else None,
              if (data.mode == JobType.createDiff) {
                ListMode.CHANGES
              } else {
                ListMode.SNAPSHOT
              },
              count
          ))
        .getOrElse(-1L)
    }
    // get a new cohortId
    cohortDefinitionId = data.existingCohortId.getOrElse(createNewCohort())

    status =
      if (cohortSizeBiggerThanLimit && request.resourceType == ResourceType.patient)
        JobExecutionStatus.LONG_PENDING
      else JobExecutionStatus.FINISHED

    //  upload into pg and solr
    if (omopTools.isDefined) {
      val cohortToUpload =
        if (data.mode == JobType.createDiff && data.modeOptions.contains(
              CreateDiffOptions.baseCohortId)) {
          val baseCohortItems =
            omopTools.get.readCohortEntries(data.modeOptions(CreateDiffOptions.baseCohortId).toLong)
          baseCohortItems
            .join(cohort,
                  baseCohortItems("_itemreferenceid") === cohort(ResultColumn.SUBJECT),
                  "full_outer")
            .filter(
              baseCohortItems("_itemreferenceid").isNull || F
                .col(ResultColumn.SUBJECT)
                .isNull)
            .select(
              F.coalesce(baseCohortItems("_itemreferenceid"), cohort(ResultColumn.SUBJECT))
                .as(ResultColumn.SUBJECT),
              F.when(cohort(ResultColumn.SUBJECT).isNull, true).otherwise(false).as("deleted")
            )
        } else {
          cohort
        }

      omopTools.get.updateCohort(
        cohortDefinitionId,
        cohortToUpload,
        completeRequest.sourcePopulation,
        count,
        cohortSizeBiggerThanLimit || data.mode == JobType.createDiff,
        request.resourceType
      )
    }

    getCreationResult(cohortDefinitionId, count, status)
  }

  private def validateModeOptionsOrThrow(data: SparkJobParameter): Unit = {
    val modeOptions = data.modeOptions
    if (data.mode == JobType.createDiff) {
      if (modeOptions.contains(CreateOptions.sampling)) {
        throw new RuntimeException("Can't use sampling with createDiff mode")
      }
      if (!modeOptions.contains(CreateDiffOptions.baseCohortId)) {
        throw new RuntimeException("baseCohortId is required for createDiff mode")
      }
    }
    if (modeOptions.contains(CreateOptions.sampling)) {
      val sampling = modeOptions(CreateOptions.sampling).toDouble
      if (sampling <= 0 || sampling > 1) {
        throw new RuntimeException("Sampling value should be between 0 and 1")
      }
    }
  }

  private def getCreationResult(cohortDefinitionId: Long,
                                count: Long,
                                status: String): JobBaseResult = {
    JobBaseResult(status,
                  Map(
                    "group.id" -> cohortDefinitionId.toString,
                    "group.count" -> count.toString,
                  ))
  }

  private def addOneEmptyGroupToRequest(request: Request): (Request, Map[Short, CriterionTags]) = {
    val completeRequest: Request =
      Request(sourcePopulation = request.sourcePopulation,
              request = Some(addEmptyGroup(List())),
              resourceType = request.resourceType)
    val completeTagsPerIdMap: Map[Short, CriterionTags] = Map(
      completeRequest.request.get.i -> new CriterionTags(false,
                                                         false,
                                                         false,
                                                         false,
                                                         List[String](),
                                                         FhirResource.PATIENT,
                                                         List[String]()))
    (completeRequest, completeTagsPerIdMap)
  }

  private def validateRequestOrThrow(data: Request): Unit = {
    if (!ResourceType.all.contains(data.resourceType)) {
      throw new RuntimeException("Resource type not supported")
    }

    if (data.resourceType != ResourceType.patient && data.request.isEmpty) {
      throw new RuntimeException("Request is empty")
    }

    if (data.resourceType != ResourceType.patient && !data.request.get
          .isInstanceOf[BasicResource]) {
      throw new RuntimeException("Non-patient resource filter request should be a basic resource")
    }
  }

}
