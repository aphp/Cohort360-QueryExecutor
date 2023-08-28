package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.tools.JobUtils.{getDefaultSolrFilterQuery, getRandomIdNotInTabooList}
import fr.aphp.id.eds.requester.tools.{OmopTools, SparkTools}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => F}

import scala.language.postfixOps

class QueryBuilderGroup(val qbBasicResource: QueryBuilderBasicResource = new QueryBuilderBasicResource()) {
  private val logger = Logger.getLogger(this.getClass)
  private val qbUtils = new QueryBuilderConfigs()
  private val qbTc = new QueryBuilderTemporalConstraint()

  /** The recursive function that can process nested basicResource or group criteria.
    *
    * @param solrConf                          solr configs extracted from SJS config
    * @param criterion                         the criterion
    * @param criterionWithTcList               list of criterion id concerned by a tc
    * @param sourcePopulation                  caresite and provider source population
    * @param tagsPerId                         map linking id to 3 info:
    *                                           - 'isDt' (tells if the criterion contains datetime information).
    *                                           - 'isEncounter' (tells if the criterion contains encounter information).
    *                                           - 'resourceType' (criterion resource name - 'default' for groups).
    * @param omopTools                         instance of object to interact with cache
    * @param ownerEntityId                     the id of the user to name the cache
    * @param enableCurrentGroupCache           whether to cache the current group or not
    * @param cacheNestedGroup                  whether to cache nested groups or not
    * */
  def processSubrequest(spark: SparkSession,
                        solrConf: Map[String, String],
                        criterion: BaseQuery,
                        sourcePopulation: SourcePopulation,
                        criterionTagsMap: Map[Short, CriterionTags],
                        omopTools: OmopTools,
                        ownerEntityId: String,
                        enableCurrentGroupCache: Boolean,
                        cacheNestedGroup: Boolean): DataFrame = {
    criterion match {
      case res: BasicResource =>
        qbBasicResource.processRequestBasicResource(spark,
                                                    solrConf,
                                                    criterionTagsMap(res._id),
                                                    sourcePopulation,
                                                    res)

      case group: GroupResource =>
        if (enableCurrentGroupCache) { // @todo: this is unused code for now
          logger.debug("cache is enabled for nested groups")
          val hashDf = omopTools.getCohortHash(criterion.toString)
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
                                                       solrConf,
                                                       criterionTagsMap,
                                                       sourcePopulation,
                                                       omopTools,
                                                       ownerEntityId,
                                                       cacheNestedGroup,
                                                       group)
              SparkTools.putCached(hashDf, ownerEntityId, groupDataFrame)
              groupDataFrame
          }
        } else {
          processRequestGroup(spark,
                              solrConf,
                              criterionTagsMap,
                              sourcePopulation,
                              omopTools,
                              ownerEntityId,
                              cacheNestedGroup,
                              group)
        }
    }
  }

  private def feedInclusionCriteriaIfEmpty(isInclusionCriteriaEmpty: Boolean,
                                           inclusionCriteria: List[BaseQuery],
                                           sourcePopulation: SourcePopulation,
                                           exclusionCriteriaId: List[Short]): List[BaseQuery] = {
    if (isInclusionCriteriaEmpty) {
      val defaultSolrFilterQuery: String = getDefaultSolrFilterQuery(sourcePopulation)
      val allTabooId: List[Short] = inclusionCriteria.map(x => x.i) ++ exclusionCriteriaId
      val newCriterionIdList: Short = getRandomIdNotInTabooList(allTabooId)
      List(
        BasicResource(newCriterionIdList,
                      isInclusive = true,
                      "patientAphp",
                      defaultSolrFilterQuery,
                      None,
                      None,
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
        addedCriterion.i -> new CriterionTags(false,
                                              false,
                                              false,
                                              List[String](),
                                              "patientAphp",
                                              List[String]()))
    else criterionTagsMap
  }

  /** Compute the resulting df of a criteria which is a group of criteria
    *
    * @param groupResource                             the group object
    * @param solrConf                          solr configs extracted from SJS config
    * @param criterionWithTcList     list of criterion id concerned by a tc
    * @param sourcePopulation                 caresite and provider source population
    * @param tagsPerId                  map linking id to their tags info.
    * @param omopTools                         instance of object to interact with cache
    * @param ownerEntityId                     the id of the user to name the cache
    * @param cacheNestedGroup                  whether it is the top level request or not.
    * */
  def processRequestGroup(spark: SparkSession,
                          solrConf: Map[String, String],
                          criterionTagsMap: Map[Short, CriterionTags],
                          sourcePopulation: SourcePopulation,
                          omopTools: OmopTools,
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
      solrConf,
      completedCriterionTagsMap,
      sourcePopulation,
      omopTools,
      ownerEntityId,
      cacheNestedGroup
    )

    val groupIdColumnName = qbUtils.getPatientColumn(groupId)
    var dfGroup =
      if (temporalConstraints.isEmpty) {
        QueryBuilderLogicalConstraint.processGroupWithoutTemporalConstraint(
          dataFramePerIdMap,
          completedCriterionTagsMap,
          groupResource,
          groupIdColumnName,
          groupId,
          inclusionCriteriaIdList)
      } else if (List("orGroup", "nAmongM").contains(groupResource._type)) {
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
    dfGroup = QueryBuilderLogicalConstraint.joinExclusionCriteria(groupIdColumnName,
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
    * @param solrConf                          solr configs extracted from SJS config
    * @param criterionTagsMap               map linking id to their tags info.
    * @param dataFramePerIdMap                        map linking id to their corresponding df
    * @param datePreferencePerIdList list of criterion id concerned by a tc
    * @param sourcePopulation                 caresite and provider source population
    * @param omopTools                         instance of object to interact with cache
    * */
  private def computeCriteria(implicit spark: SparkSession,
                              criteria: List[BaseQuery],
                              solrConf: Map[String, String],
                              criterionTagsMap: Map[Short, CriterionTags],
                              sourcePopulation: SourcePopulation,
                              omopTools: OmopTools,
                              ownerEntityId: String,
                              cacheNestedGroup: Boolean): Map[Short, DataFrame] = {
    var dataFramePerIdMapTmp = Map[Short, DataFrame]()
    for (criterion <- criteria) {
      val criterionDataframe = processSubrequest(spark,
                                                 solrConf,
                                                 criterion,
                                                 sourcePopulation,
                                                 criterionTagsMap,
                                                 omopTools,
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
