package fr.aphp.id.eds.requester.query.engine

import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.query.model._
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.query.resolver.ResourceConfig
import fr.aphp.id.eds.requester.tools.{JobUtils, JobUtilsService, SparkTools}
import fr.aphp.id.eds.requester.{FhirResource, QueryColumn}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.language.postfixOps

case class QueryExecutionOptions(resourceConfig: ResourceConfig, withOrganizations: Boolean = false)

class QueryBuilderGroup(val qbBasicResource: QueryBuilderBasicResource,
                        val options: QueryExecutionOptions,
                        val jobUtilsService: JobUtilsService = JobUtils) {
  private val logger = Logger.getLogger(this.getClass)
  private val qbTc = new QueryBuilderTemporalConstraint(options)
  private val qbLc = new QueryBuilderLogicalConstraint(options)

  /** The recursive function that can process nested basicResource or group criteria.
    *
    * @param criterion                         the criterion
    * @param criterionWithTcList               list of criterion id concerned by a tc
    * @param sourcePopulation                  caresite and provider source population
    * @param tagsPerId                         map linking id to 3 info:
    *                                           - 'isDt' (tells if the criterion contains datetime information).
    *                                           - 'isEncounter' (tells if the criterion contains encounter information).
    *                                           - 'resourceType' (criterion resource name - 'default' for groups).
    * @param ownerEntityId                     the id of the user to name the cache
    * @param enableCurrentGroupCache           whether to cache the current group or not
    * @param cacheNestedGroup                  whether to cache nested groups or not
    * */
  def processSubrequest(spark: SparkSession,
                        criterion: BaseQuery,
                        sourcePopulation: SourcePopulation,
                        criterionTagsMap: Map[Short, CriterionTags],
                        stageCounts: Option[mutable.Map[Short, Long]],
                        ownerEntityId: String,
                        enableCurrentGroupCache: Boolean,
                        cacheNestedGroup: Boolean): DataFrame = {
    val resultDf = criterion match {
      case res: BasicResource =>
        qbBasicResource.processFhirRessource(spark,
                                             sourcePopulation,
                                             criterionTagsMap(res._id),
                                             res)

      case group: GroupResource =>
        if (enableCurrentGroupCache) { // @todo: this is unused code for now
          logger.debug("cache is enabled for nested groups")
          val hashDf = JobUtils.getCohortHash(criterion.toString)
          SparkTools.getCached(spark, hashDf, ownerEntityId) match {
            case Some(groupDataFrame) =>
              if (logger.isDebugEnabled)
                logger.debug(
                  s"Df of group with _id=${group.i} is in cache : " +
                    s"df.columns=${groupDataFrame.columns.toList
                      .toString()}")
              groupDataFrame
            case _ =>
              logger.debug("DF not found in cache")
              val groupDataFrame = processRequestGroup(spark,
                                                       criterionTagsMap,
                                                       stageCounts,
                                                       sourcePopulation,
                                                       ownerEntityId,
                                                       cacheNestedGroup,
                                                       group)
              SparkTools.putCached(hashDf, ownerEntityId, groupDataFrame)
              groupDataFrame
          }
        } else {
          processRequestGroup(spark,
                              criterionTagsMap,
                              stageCounts,
                              sourcePopulation,
                              ownerEntityId,
                              cacheNestedGroup,
                              group)
        }
    }
    stageCounts.map(m => {
      // count the number of patients at each stage but only those preset in the stageCounts map
      if (m.contains(criterion.i)) {
        m.put(criterion.i, resultDf.select(QueryBuilderUtils.getSubjectColumn(criterion.i)).distinct().count())
      }
    })
    resultDf
  }

  private def feedInclusionCriteriaIfEmpty(isInclusionCriteriaEmpty: Boolean,
                                           inclusionCriteria: List[BaseQuery],
                                           sourcePopulation: SourcePopulation,
                                           exclusionCriteriaId: List[Short]): List[BaseQuery] = {
    if (isInclusionCriteriaEmpty) {
      val defaultSolrFilterQuery: String =
        qbBasicResource.querySolver.getDefaultFilterQueryPatient(sourcePopulation)
      val allTabooId: List[Short] = inclusionCriteria.map(x => x.i) ++ exclusionCriteriaId
      val newCriterionIdList: Short = jobUtilsService.getRandomIdNotInTabooList(allTabooId)
      List(
        BasicResource(newCriterionIdList,
                      isInclusive = true,
                      FhirResource.PATIENT,
                      defaultSolrFilterQuery,
                      None,
                      None,
                      None))
    } else inclusionCriteria
  }

  def feedCriterionTagsMapIfInclusionCriteriaIsEmpty(
      isInclusionCriteriaEmpty: Boolean,
      addedCriterion: BaseQuery,
      criterionTagsMap: Map[Short, CriterionTags]): Map[Short, CriterionTags] = {
    if (isInclusionCriteriaEmpty)
      criterionTagsMap ++ Map(
        addedCriterion.i -> new CriterionTags(
          false,
          false,
          false,
          false,
          List[String](),
          FhirResource.PATIENT,
          if (options.withOrganizations) {
            options.resourceConfig
              .requestKeyPerCollectionMap(FhirResource.PATIENT)
              .getOrElse(QueryColumn.ORGANIZATIONS, List())
          } else List[String](),
          withOrganizations = options.withOrganizations,
        ))
    else criterionTagsMap
  }

  /** Compute the resulting df of a criteria which is a group of criteria
    *
    * @param groupResource                             the group object
    * @param criterionWithTcList     list of criterion id concerned by a tc
    * @param sourcePopulation                 caresite and provider source population
    * @param tagsPerId                  map linking id to their tags info.
    * @param ownerEntityId                     the id of the user to name the cache
    * @param cacheNestedGroup                  whether it is the top level request or not.
    * */
  def processRequestGroup(spark: SparkSession,
                          criterionTagsMap: Map[Short, CriterionTags],
                          stageCounts: Option[mutable.Map[Short, Long]],
                          sourcePopulation: SourcePopulation,
                          ownerEntityId: String,
                          cacheNestedGroup: Boolean,
                          groupResource: GroupResource): DataFrame = {
    // extract and normalize parameters of the group
    val groupId = groupResource._id
    var inclusionCriteria = groupResource.criteria.filter(x => x.IsInclusive)
    val exclusionCriteria = groupResource.criteria.filter(x => !x.IsInclusive)
    val exclusionCriteriaId = exclusionCriteria.map(x => x.i)
    logger.debug(s"exclusionCriteriaId=$exclusionCriteriaId")

    val isInclusionCriteriaEmpty = inclusionCriteria.isEmpty
    inclusionCriteria = feedInclusionCriteriaIfEmpty(isInclusionCriteriaEmpty,
                                                     inclusionCriteria,
                                                     sourcePopulation,
                                                     exclusionCriteriaId)
    val completedCriterionTagsMap: Map[Short, CriterionTags] =
      feedCriterionTagsMapIfInclusionCriteriaIsEmpty(isInclusionCriteriaEmpty,
                                                     inclusionCriteria.head,
                                                     criterionTagsMap)
    val inclusionCriteriaIdList = inclusionCriteria.map(x => x.i)
    val criteria = inclusionCriteria ++ exclusionCriteria
    val temporalConstraints =
      groupResource.temporalConstraints.getOrElse(List[TemporalConstraint]())

    // step 1: get df associated to each criteria
    val dataFramePerIdMap = computeCriteria(
      spark,
      criteria,
      completedCriterionTagsMap,
      stageCounts,
      sourcePopulation,
      ownerEntityId,
      cacheNestedGroup
    )

    val groupIdColumnName = QueryBuilderUtils.getSubjectColumn(groupId)
    var dfGroup =
      if (temporalConstraints.isEmpty) {
        qbLc.processGroupWithoutTemporalConstraint(dataFramePerIdMap,
                                                   completedCriterionTagsMap,
                                                   groupResource,
                                                   groupIdColumnName,
                                                   groupId,
                                                   inclusionCriteriaIdList)
      } else if (List(GroupResourceType.OR, GroupResourceType.N_AMONG_M).contains(
                   groupResource._type)) {
        throw new Exception("Cannot use temporal constraints within orGroup and nAmongM groups")
      } else { // andGroup + temporal constraint
        qbTc.joinAndGroupWithTemporalConstraint(spark,
                                                groupIdColumnName,
                                                groupId,
                                                groupResource,
                                                dataFramePerIdMap,
                                                temporalConstraints,
                                                criteria,
                                                completedCriterionTagsMap)
      }

    // step 4: exclude patients of each exclusion criteria
    dfGroup = qbLc.joinExclusionCriteria(groupIdColumnName,
                                         dfGroup,
                                         exclusionCriteria,
                                         dataFramePerIdMap,
                                         completedCriterionTagsMap)
    if (logger.isDebugEnabled)
      logger.debug(
        s"Group : final join : df.columns=${dfGroup.columns.toList}, df.count=${dfGroup.count()}")

    dfGroup
  }

  /** Recover df of each criteria in the group.
    *
    * @param criteria                          list of criteria of the group
    * @param criterionTagsMap               map linking id to their tags info.
    * @param dataFramePerIdMap                        map linking id to their corresponding df
    * @param datePreferencePerIdList list of criterion id concerned by a tc
    * @param sourcePopulation                 caresite and provider source population
    * */
  private def computeCriteria(implicit spark: SparkSession,
                              criteria: List[BaseQuery],
                              criterionTagsMap: Map[Short, CriterionTags],
                              stageCounts: Option[mutable.Map[Short, Long]],
                              sourcePopulation: SourcePopulation,
                              ownerEntityId: String,
                              cacheNestedGroup: Boolean): Map[Short, DataFrame] = {
    var dataFramePerIdMapTmp = Map[Short, DataFrame]()
    for (criterion <- criteria) {
      val criterionDataframe = processSubrequest(spark,
                                                 criterion,
                                                 sourcePopulation,
                                                 criterionTagsMap,
                                                 stageCounts,
                                                 ownerEntityId,
                                                 cacheNestedGroup,
                                                 cacheNestedGroup)
      dataFramePerIdMapTmp = dataFramePerIdMapTmp + (criterion.i -> criterionDataframe)
    }
    if (logger.isDebugEnabled)
      logger.debug(s"Group : computeCriteria : dataFramePerIdMapTmp=${dataFramePerIdMapTmp.foreach(
        x => (x._1, x._2.columns.toList.toString()))}")
    dataFramePerIdMapTmp
  }

}
