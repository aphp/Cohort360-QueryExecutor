package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.{QueryColumn, SolrColumn}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.util.IntervalUtils.makeInterval
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class QueryBuilderTemporalConstraint(val options: QueryExecutionOptions = QueryExecutionOptions()) {
  private val logger = Logger.getLogger(this.getClass)
  private val qbConfigs = new QueryBuilderConfigs()
  private val qbUtils = new QueryBuilderUtils()

  def getOccurrenceChoice(temporalConstraint: TemporalConstraint,
                          idList: List[Short]): Map[Short, String] = {
    var occurrenceChoice = temporalConstraint.occurrenceChoice.getOrElse(Map[Short, String]())
    idList.foreach(i => occurrenceChoice += (i -> occurrenceChoice.getOrElse(i, "any")))
    if (logger.isDebugEnabled)
      logger.debug(s"apply_temporal_constraint : occurrenceChoice=$occurrenceChoice")
    occurrenceChoice
  }

  def getConstraintType(temporalConstraint: TemporalConstraint): String = {
    val constaintType = temporalConstraint.constraintType
    if (logger.isDebugEnabled)
      logger.debug(s"apply_temporal_constraint : constaintType=$constaintType")
    constaintType
  }

  def getIdList(temporalConstraint: TemporalConstraint,
                criteria: List[BaseQuery],
                tagsPerId: Map[Short, CriterionTags]): List[Short] = {
    def infoToCheck(idTags: CriterionTags, constraintType: String): Boolean =
      if (List[String]("sameEncounter", "differentEncounter").contains(constraintType))
        idTags.isEncounterAvailable
      else idTags.isDateTimeAvailable

    // extract criteria that are related to a date or encounter among all criteria of the group
    temporalConstraint.idList match {
      case Left(_) =>
        criteria
          .filter(x =>
            infoToCheck(tagsPerId(x.i), temporalConstraint.constraintType) && x.IsInclusive)
          .map(x => x.i)
      case Right(idList) =>
        val inclusiveCriteria =
          criteria.filter(x => x.IsInclusive).map(x => x.i)
        idList.filter(
          x =>
            infoToCheck(tagsPerId(x), temporalConstraint.constraintType) && inclusiveCriteria
              .contains(x))
    }
  }

  def getDateIsNotNull(tc: TemporalConstraint, idList: List[Short]): Map[Short, Boolean] = {
    val dateIsNotNullOpt = tc.dateIsNotNull
    val dateIsNotNull = dateIsNotNullOpt match {
      case Some(x) =>
        var dateIsNotNull = Map[Short, Boolean]()
        x match {
          case Left(a) =>
            idList.foreach(i => dateIsNotNull += (i -> a))
          case Right(b) =>
            idList.foreach(i => dateIsNotNull += (i -> b.getOrElse(i, false)))
        }
        dateIsNotNull
      case None =>
        var dateIsNotNull = Map[Short, Boolean]()
        idList.foreach(i => dateIsNotNull += (i -> false))
        dateIsNotNull
    }
    if (logger.isDebugEnabled)
      logger.debug(s"apply_temporal_constraint : dateIsNotNull=$dateIsNotNull")
    dateIsNotNull
  }

  def getDatePreference(temporalConstraint: TemporalConstraint,
                        idList: List[Short],
                        tagsPerId: Map[Short, CriterionTags]): Map[Short, List[String]] = {
    var datePreferenceMap = temporalConstraint.datePreference.getOrElse(Map[Short, List[String]]())
    idList.foreach(i => {
      val datePreference =
        if (datePreferenceMap.contains(i))
          datePreferenceMap(i)
        else
          qbConfigs.defaultDatePreferencePerCollection(tagsPerId(i).resourceType)
      datePreferenceMap += (i -> datePreference)
    })
    if (logger.isDebugEnabled)
      logger.debug(s"apply_temporal_constraint : datePreferenceMap=$datePreferenceMap")
    datePreferenceMap
  }

  private def applyTemporalConstraintOnGroupDataFrame(
      groupDataFrame: Option[DataFrame],
      temporalConstraintDataFrame: DataFrame,
      firstCriterionId: Short,
      groupId: Short,
      withOrganizations: Boolean): Option[DataFrame] = {
    // @todo: when we will enable groups to be constrained by, we will need to koin on and keep more columns (encounter_id and dates)
    // @todo: not a left semi if not "andGroup"
    val groupIdColumName = qbConfigs.getSubjectColumn(groupId)
    val criterionIdColumnName = qbConfigs.getSubjectColumn(firstCriterionId)
    val organizationColumnName = qbConfigs.getOrganizationsColumn(firstCriterionId)
    val selectedColumns = List(criterionIdColumnName) ++ (if (withOrganizations)
                                                            List(organizationColumnName)
                                                          else List())
    var patientListDataFrame =
      temporalConstraintDataFrame
        .select(selectedColumns.map(F.col): _*)
        .withColumnRenamed(criterionIdColumnName, groupIdColumName)
        .dropDuplicates(groupIdColumName)
    if (withOrganizations) {
      patientListDataFrame = patientListDataFrame
        .withColumnRenamed(qbConfigs.getOrganizationsColumn(firstCriterionId),
                           qbConfigs.getOrganizationsColumn(groupId))
    }
    patientListDataFrame = if (groupDataFrame.isEmpty) {
      patientListDataFrame
    } else {
      if (logger.isDebugEnabled)
        logger.debug(
          s"dfTcGroup.columns:${groupDataFrame.get.columns.toList}, patientListDataFrame.columns:${patientListDataFrame.columns.toList}")
      groupDataFrame.get
        .alias("a")
        .join(patientListDataFrame.alias("b"),
              F.col(s"a.$groupIdColumName") === F.col(s"b.$groupIdColumName"),
              "leftsemi")
    }
    Some(patientListDataFrame)
  }

  def processSameEncounterTc(idList: List[Short], inDictDf: Map[Short, DataFrame]): DataFrame = {
    def keepEncounterIdColumnNames(dataFrame: DataFrame): List[String] = {
      if (logger.isDebugEnabled)
        logger.debug(s"input df.columns: ${dataFrame.columns.mkString("Array(", ", ", ")")}")
      dataFrame.columns
        .filter(
          columnName =>
            columnName.contains(s"_::_${QueryColumn.ENCOUNTER}") &&
              !(columnName.contains(QueryColumn.ENCOUNTER_START_DATE) ||
                columnName.contains(QueryColumn.ENCOUNTER_END_DATE))
        )
        .toList
    }

    val firstId = idList.head
    var dfGroup = inDictDf(firstId)
    val encounterIdColumnNames = keepEncounterIdColumnNames(dfGroup)

    for (id_ <- idList.tail) {
      val dfJoin = inDictDf(id_)
      val joinEncounterCol = keepEncounterIdColumnNames(dfJoin)
      var joinOn = encounterIdColumnNames
        .flatMap(x1 => joinEncounterCol.map(x2 => dfGroup(x1) <=> dfJoin(x2)))
        .reduce(_ && _)
      // @todo : the following line useless if one patient per encounter
      joinOn = dfGroup(qbConfigs.getSubjectColumn(firstId)) <=> dfJoin(
        qbConfigs.getSubjectColumn(id_)) && joinOn
      if (logger.isDebugEnabled) logger.debug(s"joinOn: $joinOn")
      dfGroup = dfGroup.join(dfJoin, joinOn, joinType = "leftsemi")
    }
    dfGroup
  }

  def processDirectChronologicalOrderingTemporalConstraint(
      idList: List[Short],
      dataFramePerIdMap: Map[Short, DataFrame],
      maxDuration: Option[TemporalConstraintDuration],
      minDuration: Option[TemporalConstraintDuration],
      datePreferenceMap: Map[Short, List[String]],
      dateIsNotNull: Map[Short, Boolean],
      criterionTagsMap: Map[Short, CriterionTags]): DataFrame = {
    val dateTimeColumnList: List[String] =
      idList.map(i => s"localDate_${datePreferenceMap(i).mkString("")}")

    def transformAddedTime(duration: TemporalConstraintDuration): mutable.Map[String, Int] = {
      // @todo : not clean, add months and years separately
      val hours = duration.hours.getOrElse(0) + 24 * duration.days.getOrElse(0) + 24 * 30 * duration.months
        .getOrElse(0) + 24 * 365 * duration.years.getOrElse(0)
      val addedTime = mutable.Map[String, Int]()
      if (hours > 0) addedTime.put("HOURS", hours)
      if (duration.minutes.isDefined && duration.minutes.get > 0)
        addedTime.put("minutes", duration.minutes.get)
      if (duration.seconds.isDefined && duration.seconds.get > 0)
        addedTime.put("seconds", duration.seconds.get)
      addedTime
    }

    def addDateTimeColumnsToAllCriterionConcernedByTheTemporalConstraint(
        criterionTagsMap: Map[Short, CriterionTags]): Map[Short, DataFrame] = {
      var dataFrameWithDateTimeColumnsPerIdMap: Map[Short, DataFrame] = Map[Short, DataFrame]()
      for (criterionId <- idList) {
        val datePreference = datePreferenceMap(criterionId)

        val dataFrameWithDateTimeColumns = qbUtils.buildLocalDateColumn(
          dataFramePerIdMap(criterionId),
          criterionId,
          datePreference,
          criterionTagsMap(criterionId).resourceType,
          suffixe = s"_${datePreference.mkString("")}"
        )
        if (logger.isDebugEnabled)
          logger.debug(s"apply_temporal_constraint : build_local_date_column : " +
            s"_id=$criterionId, datePreference=$datePreference, groupDf.columns:${dataFrameWithDateTimeColumns.columns.toList}")
        dataFrameWithDateTimeColumnsPerIdMap = dataFrameWithDateTimeColumnsPerIdMap + (criterionId -> dataFrameWithDateTimeColumns)
      }
      dataFrameWithDateTimeColumnsPerIdMap
    }

    def joinCriteriaOnPatientId(
        dataFrameWithDateTimeColumnsPerIdMap: Map[Short, DataFrame]): DataFrame = {
      val firstId = idList.head
      var groupDf = dataFrameWithDateTimeColumnsPerIdMap(firstId)
      idList.tail.foreach(
        criterionId =>
          groupDf = groupDf.join(dataFrameWithDateTimeColumnsPerIdMap(criterionId),
                                 F.col(qbConfigs.getSubjectColumn(firstId)) === F.col(
                                   qbConfigs.getSubjectColumn(criterionId)),
                                 "inner"))
      if (logger.isDebugEnabled)
        logger.debug(
          s"apply_temporal_constraint : joinedDf.columns:${groupDf.columns.toList}, " +
            s"joinedDf.count:${groupDf.count()}")
      groupDf
    }

    def addTimeDurationToDateTimeColumn(columnName: String,
                                        duration: TemporalConstraintDuration): Column = {
      val durationToAdd = transformAddedTime(duration)
      var resultingColumn: Column = F.col(columnName).cast(TimestampType)
      durationToAdd.foreach(el =>
        resultingColumn = resultingColumn + expr(s"INTERVAL ${el._2} ${el._1}")) // List of intervals
      resultingColumn
    }

    def getDirectChronologicalOrderingSparkFilter(): Column = {
      // to realize the temporal constraint, concatenate the required constraints between couples of columns
      val filterList = ListBuffer[Column]()

      for (i_ <- 0 to idList.size - 2) {
        val (id1, id2, dt1, dt2) =
          (idList(i_), idList(i_ + 1), dateTimeColumnList(i_), dateTimeColumnList(i_ + 1))
        val criterion1DateTimeColumnName = qbConfigs.buildColName(id1, dt1)
        val criterion2DateTimeColumnName = qbConfigs.buildColName(id2, dt2)
        val defaultFilter: Column = F.col(criterion1DateTimeColumnName) <= F.col(
          criterion2DateTimeColumnName)
        if (minDuration.isDefined || maxDuration.isDefined) {
          if (minDuration.isDefined) {
            // If a minDuration is set, add all intervals between criteria 1 (id1/dt1) and criteria 2 (id2/dt2) to the filter
            val dateTimeColumnWithAddedDuration =
              addTimeDurationToDateTimeColumn(criterion1DateTimeColumnName, minDuration.get)
            val newFilter = dateTimeColumnWithAddedDuration <= F.col(criterion2DateTimeColumnName) // Criteria 2
            filterList += newFilter
          } else filterList += defaultFilter
          if (maxDuration.isDefined) {
            // If a maxDuration is set, add all intervals between criteria 1 (id1/dt1) and criteria 2 (id2/dt2) to the filter
            val dateTimeColumnWithAddedDuration =
              addTimeDurationToDateTimeColumn(criterion1DateTimeColumnName, maxDuration.get)
            val newFilter = dateTimeColumnWithAddedDuration >= F.col(criterion2DateTimeColumnName) // criteria 2
            filterList += newFilter
          }
        } else {
          // If no duration is set, only add both criteria
          filterList += defaultFilter
        }
      }
      var sparkFilter = filterList.head
      filterList.tail.foreach(x => sparkFilter = sparkFilter && x)
      if (logger.isDebugEnabled)
        logger.debug(s"apply_temporal_constraint : filter:$sparkFilter")
      sparkFilter
    }

    def addDateIsNotNullSparkFilter(sparkFilter: Column): Column = {
      var modifiedSparkFilter = sparkFilter
      idList.zipWithIndex.foreach(
        i =>
          if (!dateIsNotNull.getOrElse(i._1, false)) {
            modifiedSparkFilter = modifiedSparkFilter || F
              .col(s"${i._1}_::_${dateTimeColumnList(i._2)}")
              .isNull
        }
      )
      modifiedSparkFilter
    }

    val dataFrameWithDateTimeColumnsPerIdMap: Map[Short, DataFrame] =
      addDateTimeColumnsToAllCriterionConcernedByTheTemporalConstraint(criterionTagsMap)

    var groupDf = joinCriteriaOnPatientId(dataFrameWithDateTimeColumnsPerIdMap)

    var sparkFilter: Column = getDirectChronologicalOrderingSparkFilter()
    sparkFilter = addDateIsNotNullSparkFilter(sparkFilter)

    if (logger.isDebugEnabled)
      logger.debug(s"apply_temporal_constraint : boolean_column : filter=$sparkFilter")
    groupDf = groupDf.filter(sparkFilter)

    if (logger.isDebugEnabled) {
      logger.debug(s"apply_temporal_constraint : boolean_column : " +
        s"groupDf.count=${groupDf.count()}, groupDf.columns=${groupDf.columns.toList.toString()}, " +
        s"groupDf.head=${groupDf.collect().toList.slice(0, 10)}")
    }
    groupDf
  }

  private def normalizeGroupDataFrameAfterTemporalConstraint(
      initialDataFrame: DataFrame,
      dataFramePerIdMap: Map[Short, DataFrame],
      initialDataFrameId: Short,
      groupId: Short,
      criteriaToAddIsList: List[Short]): DataFrame = {
    // @todo: when we will enable groups to be constrained by, we will need to koin on and keep more columns (encounter_id and dates)
    // @todo: not a left semi if not "andGroup"
    val groupIdColumnName = qbConfigs.getSubjectColumn(groupId)
    val initialDataFrameIdColumnName = qbConfigs.getSubjectColumn(initialDataFrameId)
    if (logger.isDebugEnabled)
      logger.debug(
        s"dfGroup.columns:${initialDataFrame.columns.toList}, groupIdColumnName:$groupIdColumnName, initialDataFrameIdColumnName:$initialDataFrameIdColumnName")
    var resultDataFrame = initialDataFrame
      .withColumnRenamed(initialDataFrameIdColumnName, groupIdColumnName)
      .withColumnRenamed(qbConfigs.getOrganizationsColumn(initialDataFrameId),
                         qbConfigs.getOrganizationsColumn(groupId))
      .dropDuplicates()
    criteriaToAddIsList
      .filter(x => x != initialDataFrameId)
      .foreach(id_ => {
        val joinDataFrame = dataFramePerIdMap(id_)
        val joinColumnName = qbConfigs.getSubjectColumn(id_)
        resultDataFrame = resultDataFrame
          .join(joinDataFrame,
                resultDataFrame(groupIdColumnName) <=> joinDataFrame(joinColumnName),
                joinType = "leftsemi")
      })
    resultDataFrame
  }

  def joinAllCriteriaConcernedOrNotByATemporalConstraint(
      criteriaConcernedByATemporalConstraintDataFrame: Option[DataFrame],
      groupId: Short,
      criterionConcernedByATemporalConstraintIdList: List[Short],
      criterionNotConcernedByATemporalConstraintIdList: List[Short],
      dataFramePerIdMap: Map[Short, DataFrame]): DataFrame = {
    if (logger.isDebugEnabled)
      logger.debug(
        s"apply_temporal_constraint: idCriteriaWithTc:$criterionConcernedByATemporalConstraintIdList, idCriteriaWithoutTc:$criterionNotConcernedByATemporalConstraintIdList")
    if (criterionConcernedByATemporalConstraintIdList.isEmpty) {
      normalizeGroupDataFrameAfterTemporalConstraint(
        dataFramePerIdMap(criterionNotConcernedByATemporalConstraintIdList.head),
        dataFramePerIdMap,
        criterionNotConcernedByATemporalConstraintIdList.head,
        groupId,
        criterionNotConcernedByATemporalConstraintIdList
      )
    } else if (criterionConcernedByATemporalConstraintIdList.size == 1) {
      normalizeGroupDataFrameAfterTemporalConstraint(
        dataFramePerIdMap(criterionConcernedByATemporalConstraintIdList.head),
        dataFramePerIdMap,
        criterionConcernedByATemporalConstraintIdList.head,
        groupId,
        criterionNotConcernedByATemporalConstraintIdList
      )
    } else {
      normalizeGroupDataFrameAfterTemporalConstraint(
        criteriaConcernedByATemporalConstraintDataFrame.get,
        dataFramePerIdMap,
        groupId,
        groupId,
        criterionNotConcernedByATemporalConstraintIdList)
    }
  }

  def joinAndGroupWithTemporalConstraint(implicit spark: SparkSession,
                                         groupIdColumnName: String,
                                         groupId: Short,
                                         groupResource: GroupResource,
                                         dataFramePerIdMap: Map[Short, DataFrame],
                                         temporalConstraints: List[TemporalConstraint],
                                         criteria: List[BaseQuery],
                                         criterionTagsMap: Map[Short, CriterionTags]): DataFrame = {

    if (criterionTagsMap(groupId).isInTemporalConstraint)
      throw new Exception(
        "Not Implemented: a group with temporal constraints inside cannot be concerned by a temporal constraint itself")

    val withOrganizations = criterionTagsMap(groupId).withOrganizations
    // tagsPerId is updated for the criteria with id "group_id"
    // dict_df is filtered for the temporal criterion (to be used only locally so in_dict_df is returned)
    var criterionConcernedByATemporalConstraintIdList: List[Short] = List()
    var criterionConcernedByATemporalConstraintDataFrame: Option[DataFrame] = None
    temporalConstraints.foreach(temporalConstraint => {
      // step 1: parse tc parameters and standardize them
      val idList: List[Short] = getIdList(temporalConstraint, criteria, criterionTagsMap)
      val constraintType: String = getConstraintType(temporalConstraint)
      val occurrenceChoice: Map[Short, String] = getOccurrenceChoice(temporalConstraint, idList)

      criterionConcernedByATemporalConstraintIdList =
        if (idList.size > 1) (criterionConcernedByATemporalConstraintIdList ++ idList).distinct
        else criterionConcernedByATemporalConstraintIdList

      criterionConcernedByATemporalConstraintDataFrame = if (idList.size > 1) {
        val patientListOfTemporalConstraintDataFrame: DataFrame =
          if (constraintType == "sameEncounter" && occurrenceChoice
                .count(x => x._2 == "any") == occurrenceChoice.size) {
            processSameEncounterTc(idList, dataFramePerIdMap)
          } else if (constraintType == "directChronologicalOrdering" && occurrenceChoice
                       .count(x => x._2 == "any") == occurrenceChoice.size) {
            val datePreferenceMap = getDatePreference(temporalConstraint, idList, criterionTagsMap)
            val dateIsNotNull = getDateIsNotNull(temporalConstraint, idList)
            processDirectChronologicalOrderingTemporalConstraint(
              idList,
              dataFramePerIdMap,
              temporalConstraint.timeRelationMaxDuration,
              temporalConstraint.timeRelationMinDuration,
              datePreferenceMap,
              dateIsNotNull,
              criterionTagsMap
            )
          } else
            throw new Exception("required temporal constraints are not implemented")
        applyTemporalConstraintOnGroupDataFrame(criterionConcernedByATemporalConstraintDataFrame,
                                                patientListOfTemporalConstraintDataFrame,
                                                idList.head,
                                                groupId,
                                                withOrganizations)
      } else criterionConcernedByATemporalConstraintDataFrame
    })

    val criterionNotConcernedByATemporalConstraintIdList: List[Short] =
      criteria
        .filter(x => x.IsInclusive && !criterionConcernedByATemporalConstraintIdList.contains(x.i))
        .map(x => x.i)

    val selectedColumns = List(groupIdColumnName) ++ (if (withOrganizations)
      List(qbConfigs.getOrganizationsColumn(groupId))
    else List())

    joinAllCriteriaConcernedOrNotByATemporalConstraint(
      criterionConcernedByATemporalConstraintDataFrame: Option[DataFrame],
      groupId,
      criterionConcernedByATemporalConstraintIdList,
      criterionNotConcernedByATemporalConstraintIdList,
      dataFramePerIdMap
    ).select(selectedColumns.map(F.col): _*)

  }
}
