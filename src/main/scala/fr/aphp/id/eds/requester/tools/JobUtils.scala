package fr.aphp.id.eds.requester.tools

import fr.aphp.id.eds.requester.{CountOptions, FhirResource}
import fr.aphp.id.eds.requester.cohort.CohortCreation
import fr.aphp.id.eds.requester.jobs.{JobEnv, JobType, SparkJobParameter}
import fr.aphp.id.eds.requester.query.engine.QueryBuilderGroup
import fr.aphp.id.eds.requester.query.model._
import fr.aphp.id.eds.requester.query.parser.{CriterionTags, QueryParser}
import fr.aphp.id.eds.requester.query.resolver.{ResourceResolver, ResourceResolvers}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

trait JobUtilsService {
  def initSparkJobRequest(logger: Logger,
                          spark: SparkSession,
                          runtime: JobEnv,
                          data: SparkJobParameter)
    : (Request, Map[Short, CriterionTags], Option[CohortCreation], ResourceResolver, Boolean) = {
    logger.debug(s"Received data: ${data.toString}")

    // init db connectors
    val maybeCohortCreationService = getCohortCreationService(data, spark)
    val resourceResolver = getResourceResolver(data)

    // load input json into object
    val (request, criterionTagsMap) = QueryParser.parse(
      data.cohortDefinitionSyntax,
      QueryParsingOptions(
        resourceConfig = resourceResolver.getConfig,
        withOrganizationDetails = data.mode == JobType.countWithDetails,
        useFilterSolr = data.resolver == ResourceResolvers.solr,
      )
    )

    logger.info(
      s"ENTER NEW QUERY JOB : ${data.toString}. Parsed criterionIdWithTcList: $criterionTagsMap")

    (request,
     criterionTagsMap,
     maybeCohortCreationService,
     resourceResolver,
     runtime.contextConfig.business.enableCache)
  }

  def getCohortCreationService(data: SparkJobParameter, spark: SparkSession): Option[CohortCreation]

  def getResourceResolver(data: SparkJobParameter): ResourceResolver

  def getRandomIdNotInTabooList(allTabooId: List[Short], negative: Boolean = true): Short

  def getCohortHash(str: String): String = {
    import java.security.MessageDigest
    MessageDigest
      .getInstance("MD5")
      .digest(str.getBytes)
      .map("%02X".format(_))
      .mkString
  }

  /**
   * Prepare the request by wrapping the first group in an AND group if it is not inclusive and
   * fill in virtual groups in the query for non inclusive criteria within OR and N_AMONG_M groups.
   * This is because of the current implementation of the query engine that does not handle exclusion criteria within OR groups.
   * @param request the request to process
   * @param criterionTagsMap the map of criterion tags
   * @return the updated query and criterion tags map
   */
  def prepareRequest(
      request: Request,
      criterionTagsMap: Map[Short, CriterionTags]
  ): (BaseQuery, Map[Short, CriterionTags]) = {
    // wrap the first group in a higher group is it is not inclusive
    val (root, updatedCriteriaTagsMap) = if (!request.request.get.IsInclusive) {
      wrapCriteriaInAndGroup(request.request.get, mutable.Map(criterionTagsMap.toSeq: _*))
    } else {
      (request.request.get, mutable.Map(criterionTagsMap.toSeq: _*))
    }
    val (updatedRoot, updatedCriterionTagsMap) = fillInVirtualGroups(root, updatedCriteriaTagsMap)
    (updatedRoot, Map(updatedCriterionTagsMap.toSeq: _*))
  }

  private def wrapCriteriaInAndGroup(criteria: BaseQuery, criteriaTagsMap: mutable.Map[Short, CriterionTags], replaceOriginalId: Boolean = false) = {
    val newCriteriaId = getRandomIdNotInTabooList(criteriaTagsMap.keySet.toList, negative = !replaceOriginalId || criteria.i < 0)
    // create a copy of criteria with the new id
    val updatedCriteria = if (replaceOriginalId) { criteria match {
      case group: GroupResource =>
        group.copy(_id = newCriteriaId)
      case basic: BasicResource =>
        basic.copy(_id = newCriteriaId)
    }} else {
      criteria
    }
    val criteriaTagId = if (replaceOriginalId) criteria.i else newCriteriaId
    val oldCriteriaId = if (replaceOriginalId) newCriteriaId else criteria.i
    if (replaceOriginalId) {
      criteriaTagsMap(newCriteriaId) = criteriaTagsMap(criteria.i)
    }
    (GroupResource(
       groupType = GroupResourceType.AND,
       _id = if (replaceOriginalId) criteria.i else newCriteriaId,
       isInclusive = true,
       criteria = List(
         updatedCriteria
       )
     ),
     criteriaTagsMap ++ Map(
        criteriaTagId -> CriterionTags(
         criteriaTagsMap(oldCriteriaId).isDateTimeAvailable,
         criteriaTagsMap(oldCriteriaId).isEncounterAvailable,
         criteriaTagsMap(oldCriteriaId).isEpisodeOfCareAvailable,
         isInTemporalConstraint = false,
         withOrganizations = criteriaTagsMap(oldCriteriaId).withOrganizations
       )
     ))
  }

  /**
    * Fill in virtual groups in the query.
    * This is mainly used to handle exclusion criteria in OR and N_AMONG_M groups.
    * We create a new AND group with a basic criteria of all the population and the original criteria to exclude.
    * TODO The original criteria id of a criteria to exclude is passed to the new group.
    * @param query the root query to process
    * @param criterionTagsMap the map of criterion tags
    * @return the updated query and criterion tags map
    */
  private def fillInVirtualGroups(
      query: BaseQuery,
      criterionTagsMap: mutable.Map[Short, CriterionTags]): (BaseQuery, mutable.Map[Short, CriterionTags]) = {
    query match {
      case group: GroupResource =>
        // we only need to process OR and N_AMONG_M groups
        val res = group.criteria.foldLeft((List[BaseQuery](), criterionTagsMap)) {
          // for each criteria, we fill in virtual groups if the criteria is not inclusive
          case ((updatedCriteriaList, updatedCriteriaTags), c) =>
            val (updatedCriteria, _updatedCriteriaTags) = fillInVirtualGroups(c, updatedCriteriaTags)
            if ((group.groupType == GroupResourceType.N_AMONG_M || group.groupType == GroupResourceType.OR) && !c.IsInclusive) {
              val wrappedCriteria = wrapCriteriaInAndGroup(updatedCriteria, _updatedCriteriaTags, replaceOriginalId = true)
              (updatedCriteriaList :+ wrappedCriteria._1, wrappedCriteria._2)
            } else {
              (updatedCriteriaList :+ updatedCriteria, _updatedCriteriaTags)
            }
        }
        (group.copy(criteria = res._1), res._2)
      case _ => (query, criterionTagsMap)
    }
  }
}

object JobUtils extends JobUtilsService {

  def getCohortCreationService(data: SparkJobParameter,
                               spark: SparkSession): Option[CohortCreation] =
    CohortCreation.get(data.cohortCreationService)(spark)

  def getResourceResolver(data: SparkJobParameter): ResourceResolver =
    ResourceResolver.get(data.resolver, data.resolverOpts)

  def getRandomIdNotInTabooList(allTabooId: List[Short], negative: Boolean = true): Short = {
    val rnd = new scala.util.Random
    var id: Option[Short] = None
    while (id.isEmpty) {
      val rndShort = rnd.nextInt(Short.MaxValue).toShort
      val rndId: Int = if (negative) -rndShort else rndShort
      if (!allTabooId.contains(rndId)) id = Some(rndId.toShort)
    }
    id.get
  }

  def addEmptyGroup(allTabooId: List[Short]): BaseQuery = {
    GroupResource(groupType = GroupResourceType.AND,
                  _id = getRandomIdNotInTabooList(allTabooId),
                  isInclusive = true,
                  criteria = List())
  }

  def initStageCounts(modeOptions: Map[String, String],
                      request: Request): Option[mutable.Map[Short, Long]] = {
    if (modeOptions.contains(CountOptions.details)) {
      if (modeOptions(CountOptions.details).contains("all")) {
        Some(mutable.Map(JobUtils.getAllCriteriaIds(request).map(x => x -> -1L): _*))
      } else {
        Some(
          mutable.Map(
            modeOptions(CountOptions.details).strip().split(",").map(x => x.toShort -> -1L): _*))
      }
    } else {
      None
    }
  }

  private def getAllCriteriaIds(request: Request): List[Short] = {
    def getCriteriaIds(baseQuery: BaseQuery): List[Short] = baseQuery match {
      case groupResource: GroupResource =>
        groupResource.criteria.flatMap(getCriteriaIds) ++ List(groupResource._id)
      case criterion: BasicResource =>
        List(criterion._id)
    }
    request.request.map(getCriteriaIds).getOrElse(List())
  }

}
