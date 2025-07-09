package fr.aphp.id.eds.requester.query.engine

import fr.aphp.id.eds.requester.query.model.{BaseQuery, GroupResource, GroupResourceType}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, functions => F}

class QueryBuilderLogicalConstraint(val options: QueryExecutionOptions) {

  private val qbUtils = new QueryBuilderUtils(options.resourceConfig)
  private val logger = Logger.getLogger(this.getClass)

  def processGroupWithoutTemporalConstraint(dataFramePerIdMap: Map[Short, DataFrame],
                                            criterionTagsMap: Map[Short, CriterionTags],
                                            groupResource: GroupResource,
                                            groupIdColumnName: String,
                                            groupId: Short,
                                            inclusionCriteriaIdList: List[Short]): DataFrame = {
    groupResource._type match {
      case GroupResourceType.AND =>
        joinInclusionCriteriaForAndGroup(groupIdColumnName,
                                         groupId,
                                         groupResource,
                                         inclusionCriteriaIdList,
                                         dataFramePerIdMap,
                                         criterionTagsMap)
      case _ =>
        joinInclusionCriteriaForOrNAmongMGroup(groupIdColumnName,
                                               groupId,
                                               groupResource,
                                               inclusionCriteriaIdList,
                                               dataFramePerIdMap,
                                               criterionTagsMap)
    }
  }

  /** Build group df by joining inclusive criteria
    *
    * @param groupIdColumnName             column name on which joining the df
    * @param groupId              if of the group
    * @param groupResource                the group object
    * @param inclusionCriteriaId  list of inclusive criteria id of the group
    * @param dataFramePerIdMap               map linking id to their corresponding df
    * @param tagsPerId            map linking id to their tags info.
    * @param isGroupInTemporalConstraint            is the current group concerned by a temporal constraint or not
    */
  private def joinInclusionCriteriaForOrNAmongMGroup(
      groupIdColumnName: String,
      groupId: Short,
      groupResource: GroupResource,
      inclusionCriteriaId: List[Short],
      dataFramePerIdMap: Map[Short, DataFrame],
      criterionTagsMap: Map[Short, CriterionTags]): DataFrame = {
    val forceRenamingDateTimeColumns = true
    val isGroupInTemporalConstraint = criterionTagsMap(groupId).isInTemporalConstraint
    val withOrganizations = criterionTagsMap(groupId).withOrganizations
    val firstId = inclusionCriteriaId.head
    val doWeNeedAFilteringDataFrame
      : Boolean = groupResource._type == GroupResourceType.N_AMONG_M && isGroupInTemporalConstraint
    val selectedColumns = List(groupIdColumnName) ++ (if (criterionTagsMap(groupId).withOrganizations)
                                                        List(
                                                          QueryBuilderUtils.getOrganizationsColumn(
                                                            groupId))
                                                      else List())

    def initGroupDataFrame(): DataFrame = {
      normalizeColumnNamesInGroupDataFrame(dataFramePerIdMap(firstId),
                                           firstId,
                                           groupId,
                                           forceRenamingDateTimeColumns)
    }

    def initFilteringDataFrame(groupDataFrame: DataFrame): Option[DataFrame] = {
      if (doWeNeedAFilteringDataFrame) {
        val firstCol = QueryBuilderUtils.getSubjectColumn(firstId)
        Some(
          groupDataFrame
            .select(groupIdColumnName)
            .withColumnRenamed(firstCol, groupIdColumnName)
            .dropDuplicates()
        )
      } else None
    }

    def processGroupDataFrameForNAmongMGroup(groupDataFrame: DataFrame,
                                             filteringDataFrame: Option[DataFrame]): DataFrame = {
      var (n, operator) =
        (groupResource.nAmongMOptions.get.n, groupResource.nAmongMOptions.get.operator)
      operator = if (operator == "=") "==" else operator
      if (logger.isDebugEnabled)
        logger.debug(
          s"Group : join inclusion criteria NAmongM: " +
            s"n=$n, operator=$operator, df_loc.count=${groupDataFrame.count()}")
      if (isGroupInTemporalConstraint || withOrganizations) {
        val patientListDataFrame: DataFrame = filteringDataFrame.get
          .select(F.col(groupIdColumnName))
          .groupBy(groupIdColumnName)
          .count()
          .filter(s"count $operator $n")
          .drop("count")
        val filteredDf = groupDataFrame.join(
          patientListDataFrame,
          groupDataFrame(groupIdColumnName) === patientListDataFrame(groupIdColumnName),
          "left_semi")
        qbUtils.cleanDataFrame(filteredDf,
                               isGroupInTemporalConstraint,
                               selectedColumns,
                               groupIdColumnName)
      } else {
        groupDataFrame
          .groupBy(groupIdColumnName)
          .count()
          .filter(s"count $operator $n")
          .drop("count")
      }
    }

    var groupDataFrame: DataFrame = initGroupDataFrame()
    // @todo : "nAmongM && groupInTc" behaviour unknown by PO : here we keep all occurrence of criteria in the group (like orGroup)
    var filteringDataFrame: Option[DataFrame] = initFilteringDataFrame(groupDataFrame)

    for (id_ <- inclusionCriteriaId.tail) {
      if (logger.isDebugEnabled)
        logger.debug(s"Group : join inclusion criteria : " +
          s"group_id=$groupIdColumnName, groupDataFrame.count=${groupDataFrame.count}, _id=$id_, " +
          s"df_local.count=${dataFramePerIdMap(id_).count}, " +
          s"groupDataFrame.columns=${groupDataFrame.columns.toList.toString()}, " +
          s"df_local.columns=${dataFramePerIdMap(id_).columns.toList.toString()}, " +
          s"groupDataFrame.head=${groupDataFrame.head(10).toList.slice(0, 10)}")

      val normalizedCriterionDataFrame = normalizeColumnNamesInGroupDataFrame(
        dataFramePerIdMap(id_),
        id_,
        groupId,
        forceRenamingDateTimeColumns)
      groupDataFrame =
        groupDataFrame.unionByName(normalizedCriterionDataFrame, allowMissingColumns = true)
      filteringDataFrame =
        if (groupResource._type == GroupResourceType.N_AMONG_M && isGroupInTemporalConstraint)
          Some(
            filteringDataFrame.get.unionByName(
              normalizedCriterionDataFrame.select(groupIdColumnName).dropDuplicates(),
              allowMissingColumns = true))
        else None
    }

    // filter groupDataFrame specifically for nAmongM groups
    if (logger.isDebugEnabled)
      logger.debug(
        s"Group : join inclusion criteria : groupDataFrame.count=${groupDataFrame.count()}")
    groupDataFrame = groupResource._type match {
      case GroupResourceType.N_AMONG_M =>
        processGroupDataFrameForNAmongMGroup(groupDataFrame, filteringDataFrame)
      case GroupResourceType.OR =>
        qbUtils.cleanDataFrame(groupDataFrame,
                               isGroupInTemporalConstraint,
                               selectedColumns,
                               groupIdColumnName)

    }

    groupDataFrame
  }

  /** Build group df by joining inclusive criteria
    *
    * @param groupIdColumnName             column name on which joining the df
    * @param groupId              if of the group
    * @param groupResource                the group object
    * @param inclusionCriteriaId  list of inclusive criteria id of the group
    * @param dataFramePerIdMap               map linking id to their corresponding df
    * @param criterionTagsMap            map linking id to their tags info.
    * @param groupInTc            is the current group concerned by a temporal constraint or not
    */
  private def joinInclusionCriteriaForAndGroup(
      groupIdColumnName: String,
      groupId: Short,
      groupResource: GroupResource,
      inclusionCriteriaId: List[Short],
      dataFramePerIdMap: Map[Short, DataFrame],
      criterionTagsMap: Map[Short, CriterionTags]): DataFrame = {
    // build groupDataFrame
    val firstId = inclusionCriteriaId.head
    val firstCriterionIdColumnName = QueryBuilderUtils.getSubjectColumn(firstId)
    val isGroupInTemporalConstraint: Boolean = criterionTagsMap(groupId).isInTemporalConstraint
    val doWeNeedLongInsteadOfWideDataFrame: Boolean =
      criterionTagsMap(groupId).temporalConstraintTypeList.contains("directChronologicalOrdering")
    val selectedColumns = List(groupIdColumnName) ++ (if (criterionTagsMap(groupId).withOrganizations)
                                                        List(
                                                          QueryBuilderUtils.getOrganizationsColumn(
                                                            groupId))
                                                      else List())

    def initGroupDataFrame(): DataFrame = {
      val normalizedDf =
        normalizeColumnNamesInGroupDataFrame(dataFramePerIdMap(firstId), firstId, groupId, false)
      if (doWeNeedLongInsteadOfWideDataFrame)
        normalizedDf
          .select(groupIdColumnName)
      else
        normalizedDf
    }

    def updateGroupDataFrame(groupDataFrame: DataFrame, criterionId: Short): DataFrame = {
      val dfTmpJoin: DataFrame =
        if (doWeNeedLongInsteadOfWideDataFrame)
          dataFramePerIdMap(criterionId).select(QueryBuilderUtils.getSubjectColumn(criterionId))
        else dataFramePerIdMap(criterionId)

      groupDataFrame.join(dfTmpJoin,
                          groupDataFrame(groupIdColumnName) === dfTmpJoin(
                            QueryBuilderUtils.getSubjectColumn(criterionId)),
                          "inner")
    }

    def getLongInsteadOfWideDataFrame(groupDataFrame: DataFrame): DataFrame = {
      val renameDatetimeColumns = true
      val patientListDataFrame: DataFrame =
        groupDataFrame.select(groupIdColumnName).dropDuplicates()
      var longGroupDataFrame: DataFrame = dataFramePerIdMap(firstId).join(
        patientListDataFrame,
        dataFramePerIdMap(firstId)(firstCriterionIdColumnName) === patientListDataFrame(
          groupIdColumnName),
        "left_semi")
      longGroupDataFrame = normalizeColumnNamesInGroupDataFrame(longGroupDataFrame,
                                                                firstId,
                                                                groupId,
                                                                renameDatetimeColumns)
      inclusionCriteriaId.tail.foreach(criterionId => {
        var additionalDataFrame: DataFrame =
          dataFramePerIdMap(criterionId).join(
            patientListDataFrame,
            dataFramePerIdMap(criterionId)(
              QueryBuilderUtils
                .getSubjectColumn(criterionId)) === patientListDataFrame(groupIdColumnName),
            "left_semi"
          )
        additionalDataFrame = normalizeColumnNamesInGroupDataFrame(additionalDataFrame,
                                                                   criterionId,
                                                                   groupId,
                                                                   renameDatetimeColumns)
        longGroupDataFrame =
          longGroupDataFrame.unionByName(additionalDataFrame, allowMissingColumns = true)
      })
      longGroupDataFrame
    }

    var groupDataFrame: DataFrame = initGroupDataFrame()

    for (id_ <- inclusionCriteriaId.tail) {
      groupDataFrame = updateGroupDataFrame(groupDataFrame, id_)
    }

    val cleanedGroupDataFrame: DataFrame =
      if (doWeNeedLongInsteadOfWideDataFrame) {
        getLongInsteadOfWideDataFrame(groupDataFrame)
      } else {
        qbUtils.cleanDataFrame(groupDataFrame,
                               isGroupInTemporalConstraint,
                               selectedColumns,
                               groupIdColumnName)
      }

    if (logger.isDebugEnabled)
      logger.debug(s"Group : join inclusion criteria : " +
        s" groupDataFrame.count=${cleanedGroupDataFrame.count}, groupDataFrame.columns=${cleanedGroupDataFrame.columns.toList}")

    cleanedGroupDataFrame
  }

  /** Removing patients of the group dataframe that are in exclsusive criteria.
    *
    * @param groupCol          name of the column on which group df are joined
    * @param groupDataFrame        the df joining all df of the group
    * @param exclusionCriteria list fo exclusion criteria
    * @param dataFramePerIdMap            map linking criteria to their corresponding df
    * */
  def joinExclusionCriteria(groupCol: String,
                            groupDataFrame: DataFrame,
                            exclusionCriteria: List[BaseQuery],
                            dataFramePerIdMap: Map[Short, DataFrame],
                            criterionTagsMap: Map[Short, CriterionTags]): DataFrame = {
    var modifyingGroupDataFrame = groupDataFrame
    if (logger.isDebugEnabled)
      logger.debug(
        s"JOIN EXCLUSION CRITERIA : modifyingGroupDataFrame.count=${modifyingGroupDataFrame.count()}, ")
    for (exclusionCriterion <- exclusionCriteria) {
      val patientColumnName = QueryBuilderUtils.getSubjectColumn(exclusionCriterion.i)
      var joiningDataFrame = dataFramePerIdMap(exclusionCriterion.i)
      // @todo : not clean, temporal constraint on exclusion criteria is forbidden for now
      if (!criterionTagsMap(exclusionCriterion.i).isInTemporalConstraint) {
        joiningDataFrame = joiningDataFrame.select(F.col(patientColumnName))
      }
      modifyingGroupDataFrame = modifyingGroupDataFrame.join(
        joiningDataFrame,
        modifyingGroupDataFrame(groupCol) === joiningDataFrame(patientColumnName),
        "left_anti")
      if (logger.isDebugEnabled)
        logger.debug(s"JOIN EXCLUSION CRITERIA : joiningDataFrame.count=${joiningDataFrame
          .count()}, modifyingGroupDataFrame.count=${modifyingGroupDataFrame.count()}, ")
    }
    modifyingGroupDataFrame
  }

  private def normalizeColumnNamesInGroupDataFrame(sourceDataframe: DataFrame,
                                                   sourceId: Short,
                                                   targetId: Short,
                                                   renamingDateTimeColumns: Boolean): DataFrame = {
    val availableColumnNameList: List[String] = sourceDataframe.columns.toList

    def renameColumn(dataFrame: DataFrame,
                     sourceColumnName: String,
                     targetColumnName: String): DataFrame = {
      if (availableColumnNameList.contains(sourceColumnName) && renamingDateTimeColumns)
        dataFrame.withColumnRenamed(sourceColumnName, targetColumnName)
      else dataFrame
    }
    var targetDataframe: DataFrame = sourceDataframe
      .withColumnRenamed(QueryBuilderUtils.getSubjectColumn(sourceId),
                         QueryBuilderUtils.getSubjectColumn(targetId))
      .withColumnRenamed(QueryBuilderUtils.getOrganizationsColumn(sourceId),
                         QueryBuilderUtils.getOrganizationsColumn(targetId))
    targetDataframe = renameColumn(targetDataframe,
                                   QueryBuilderUtils.getEncounterColumn(sourceId),
                                   QueryBuilderUtils.getEncounterColumn(targetId))
    targetDataframe = renameColumn(targetDataframe,
                                   QueryBuilderUtils.getEncounterStartDateColumn(sourceId),
                                   QueryBuilderUtils.getEncounterStartDateColumn(targetId))
    targetDataframe = renameColumn(targetDataframe,
                                   QueryBuilderUtils.getEncounterEndDateColumn(sourceId),
                                   QueryBuilderUtils.getEncounterEndDateColumn(targetId))
    targetDataframe = renameColumn(targetDataframe,
                                   QueryBuilderUtils.getEventDateColumn(sourceId),
                                   QueryBuilderUtils.getEventDateColumn(targetId))
    targetDataframe
  }
}
