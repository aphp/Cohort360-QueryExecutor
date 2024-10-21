package fr.aphp.id.eds.requester.query.engine

import fr.aphp.id.eds.requester.query.model._
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.query.resolver.ResourceConfig
import fr.aphp.id.eds.requester.tools.{JobUtils, JobUtilsService, SparkTools}
import fr.aphp.id.eds.requester.{FhirResource, QueryColumn}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    * @param criterionTagsMap                  list of criterion tags info
    * @param context                           the context of the query
    * */
  def processSubrequest(criterion: BaseQuery,
                        criterionTagsMap: Map[Short, CriterionTags],
                        context: QueryContext): DataFrame = {
    val resultDf = criterion match {
      case res: BasicResource =>
        qbBasicResource.processFhirRessource(context.sparkSession,
                                             context.sourcePopulation,
                                             criterionTagsMap(res._id),
                                             res)

      case group: GroupResource =>
        if (context.cacheConfig.enableCurrentGroupCache) { // @todo: this is unused code for now
          logger.debug("cache is enabled for nested groups")
          val hashDf = JobUtils.getCohortHash(criterion.toString)
          SparkTools.getCached(context.sparkSession, hashDf, context.cacheConfig.ownerEntityId) match {
            case Some(groupDataFrame) =>
              if (logger.isDebugEnabled)
                logger.debug(
                  s"Df of group with _id=${group.i} is in cache : " +
                    s"df.columns=${groupDataFrame.columns.toList
                      .toString()}")
              groupDataFrame
            case _ =>
              logger.debug("DF not found in cache")
              val groupDataFrame = processRequestGroup(group, criterionTagsMap, context)
              SparkTools.putCached(hashDf, context.cacheConfig.ownerEntityId, groupDataFrame)
              groupDataFrame
          }
        } else {
          processRequestGroup(group, criterionTagsMap, context)
        }
    }
    context.stageCounts.map(m => {
      // count the number of patients at each stage but only those preset in the stageCounts map
      if (m.contains(criterion.i)) {
        val criterionSelfCount = resultDf.select(QueryBuilderUtils.getSubjectColumn(criterion.i)).distinct().count()
        val criterionCount = if (criterion.IsInclusive) {
          criterionSelfCount
        } else {
          context.sourcePopulationCount - criterionSelfCount
        }
        m.put(criterion.i, criterionCount)
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
      val newCriterionIdList: Short =
        jobUtilsService.getRandomIdNotInTabooList(allTabooId, negative = false)
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

  private def feedCriterionTagsMapIfInclusionCriteriaIsEmpty(
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
    * @param criterionTagsMap     list of criterion tags info
    * @param context                                  the context of the query
    * */
  private def processRequestGroup(groupResource: GroupResource,
                          criterionTagsMap: Map[Short, CriterionTags],
                          context: QueryContext): DataFrame = {
    // extract and normalize parameters of the group
    val groupId = groupResource._id
    var inclusionCriteria = groupResource.criteria.filter(x => x.IsInclusive)
    val exclusionCriteria = groupResource.criteria.filter(x => !x.IsInclusive)
    val exclusionCriteriaId = exclusionCriteria.map(x => x.i)
    logger.debug(s"exclusionCriteriaId=$exclusionCriteriaId")

    val isInclusionCriteriaEmpty = inclusionCriteria.isEmpty
    inclusionCriteria = feedInclusionCriteriaIfEmpty(isInclusionCriteriaEmpty,
                                                     inclusionCriteria,
                                                     context.sourcePopulation,
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
      criteria,
      completedCriterionTagsMap,
      context
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
        qbTc.joinAndGroupWithTemporalConstraint(context.sparkSession,
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
    * @param context                          the context of the query
    * */
  private def computeCriteria(criteria: List[BaseQuery],
                              criterionTagsMap: Map[Short, CriterionTags],
                              context: QueryContext): Map[Short, DataFrame] = {
    var dataFramePerIdMapTmp = Map[Short, DataFrame]()
    for (criterion <- criteria) {
      val criterionDataframe = processSubrequest(
        criterion,
        criterionTagsMap,
        context
      )
      dataFramePerIdMapTmp = dataFramePerIdMapTmp + (criterion.i -> criterionDataframe)
    }
    if (logger.isDebugEnabled)
      logger.debug(s"Group : computeCriteria : dataFramePerIdMapTmp=${dataFramePerIdMapTmp.foreach(
        x => (x._1, x._2.columns.toList.toString()))}")
    dataFramePerIdMapTmp
  }

}
