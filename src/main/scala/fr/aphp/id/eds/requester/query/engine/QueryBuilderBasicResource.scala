package fr.aphp.id.eds.requester.query.engine

import fr.aphp.id.eds.requester._
import fr.aphp.id.eds.requester.query.model.{BasicResource, DateRange, PatientAge, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.query.resolver.{ResourceConfig, ResourceResolver}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}

import scala.collection.mutable.ListBuffer

class QueryBuilderBasicResource(val querySolver: ResourceResolver) {
  private val logger = Logger.getLogger(this.getClass)
  private val qbConfigs: ResourceConfig = querySolver.getConfig
  private val qbUtils: QueryBuilderUtils = new QueryBuilderUtils(qbConfigs)

  /** Filter patient of input dataframe based on the date of the occurrence
    *
    * @param criterionDataFrame resulting dataframe of patient of a basicResource
    * @param basicResource      basicResource object
    * @param criterionId        id of the basicResource
    * */
  private def filterByDateRangeList(criterionDataFrame: DataFrame,
                                    basicResource: BasicResource,
                                    criterionId: Short): DataFrame = {
    var dateRangeList = basicResource.dateRangeList

    def addEncounterDateRangeToDateRangeList(): Option[List[DateRange]] = {
      val encounterRangeList: ListBuffer[DateRange] = new ListBuffer[DateRange]()
      if (basicResource.encounterDateRange.isDefined) {
        val encounterDateRange = basicResource.encounterDateRange.get
        if (encounterDateRange.minDate.isDefined) {
          logger.info(s"****** Encounter date range min: ${encounterDateRange.minDate} ")
          encounterRangeList += DateRange(
            minDate = encounterDateRange.minDate,
            datePreference = Some(List(QueryColumn.ENCOUNTER_START_DATE)),
            dateIsNotNull = encounterDateRange.dateIsNotNull,
            maxDate = None
          )
        }
        if (encounterDateRange.maxDate.isDefined) {
          logger.info(s"****** Encounter date range max: ${encounterDateRange.maxDate} ")
          encounterRangeList += DateRange(
            maxDate = encounterDateRange.maxDate,
            datePreference = Some(List(QueryColumn.ENCOUNTER_END_DATE)),
            dateIsNotNull = encounterDateRange.dateIsNotNull,
            minDate = None
          )
        }
      }
      if (dateRangeList.isDefined) {
        logger.info(s"****** dateRangeList is Defined")
        logger.info(
          s"****** Date range list is defined: ${dateRangeList.get ++ encounterRangeList.toList}")
        Some(dateRangeList.get ++ encounterRangeList.toList)
      } else if (encounterRangeList.nonEmpty) {
        logger.info(s"****** encounterRangeList nonEmpty")
        Some(encounterRangeList.toList)
      } else {
        None
      }
    }

    def getDateRangeSparkFilter(dateRange: DateRange, dateIsNotNull: Boolean): Column = {
      val sparkFilterList = new ListBuffer[Column]()
      if (dateRange.maxDate.isDefined)
        sparkFilterList += F.col(QueryBuilderUtils.getDateColumn(criterionId)) <= s"${dateRange.maxDate.get}"
      if (dateRange.minDate.isDefined)
        sparkFilterList += F.col(QueryBuilderUtils.getDateColumn(criterionId)) >= s"${dateRange.minDate.get}"
      val unifiedSparkFilter: Column = sparkFilterList.toList.reduce(_ && _)
      if (!dateIsNotNull) // F.col(qbConfigs.getDateColumn(criterionId)).isNull
        unifiedSparkFilter || F.col(QueryBuilderUtils.getDateColumn(criterionId)).isNull
      else unifiedSparkFilter
    }

    dateRangeList = addEncounterDateRangeToDateRangeList()

    if (dateRangeList.isDefined) {
      var filteredCriterionDataFrame: DataFrame = criterionDataFrame
      for (dateRange <- dateRangeList.get) {
        val datePreference =
          dateRange.datePreference.getOrElse(
            QueryBuilderUtils.defaultDatePreferencePerCollection(basicResource.resourceType))
        val dateIsNotNull = dateRange.dateIsNotNull.getOrElse(true)
        filteredCriterionDataFrame = qbUtils.buildLocalDateColumn(filteredCriterionDataFrame,
                                                                  criterionId,
                                                                  datePreference,
                                                                  basicResource.resourceType)
        val unifiedSparkFilter: Column =
          getDateRangeSparkFilter(dateRange, dateIsNotNull)

        if (logger.isDebugEnabled) {
          logger.debug(s"Basic Resource with _id=$criterionId : filterByDateRangeList : " +
            s"filteredCriterionDataFrame.columns=${filteredCriterionDataFrame.columns.toList} and " +
            s"filteredCriterionDataFrame.schema=${filteredCriterionDataFrame.printSchema()} and " +
            s"filteredCriterionDataFrame.count=${filteredCriterionDataFrame.count} and " +
            s"filteredCriterionDataFrame.head=${filteredCriterionDataFrame.head(10).toList.slice(0, 10)} and " +
            s"filter=${unifiedSparkFilter.toString}")
        }
        filteredCriterionDataFrame = filteredCriterionDataFrame.where(unifiedSparkFilter === true)
        if (logger.isDebugEnabled) {
          logger.debug(
            s"Basic Resource with _id=$criterionId: " +
              s"filterByDateRangeList: df_output.count=${filteredCriterionDataFrame.count}")
        }
      }
      filteredCriterionDataFrame
    } else criterionDataFrame
  }

  /** Filter patient of input dataframe based on the age of patient at the date of the occurrence
    *
    * @param criterionDataFrame resulting dataframe of patient of a basicResource
    * @param basicResource      basicResource object
    * @param criterionId        id of the basicResource
    * */
  private def filterByPatientAge(criterionDataFrame: DataFrame,
                                 basicResource: BasicResource,
                                 criterionId: Short): DataFrame = {
    def dropTemporaryAgeColumns(criterionDataFrameWithAgeColumn: DataFrame): DataFrame = {
      val selectedColumns = criterionDataFrameWithAgeColumn.columns
        .filter(c =>
          !List(QueryColumn.AGE, QueryBuilderUtils.getPatientBirthColumn(criterionId)).contains(c))
        .map(x => F.col(x))
        .toList
      criterionDataFrameWithAgeColumn.select(selectedColumns: _*)
    }

    def getDecomposedAgeMinAndMax(patientAge: PatientAge) = {
      val Array(year_min, month_min, day_min) =
        patientAge.minAge.getOrElse("0-0-0").split("-").map(_.toInt)
      val Array(year_max, month_max, day_max) =
        patientAge.maxAge.getOrElse("0-0-0").split("-").map(_.toInt)
      (year_min, month_min, day_min, year_max, month_max, day_max)
    }

    def addAgeColumn(dataFrame: DataFrame, patientAge: PatientAge): DataFrame = {
      val (year_min, _, _, year_max, _, _) = getDecomposedAgeMinAndMax(patientAge)
      if (year_max == 0 & year_min == 0) {
        dataFrame.withColumn(
          QueryColumn.AGE,
          F.datediff(F.col(s"${QueryBuilderUtils.getDateColumn(criterionId)}"),
                     F.col(QueryBuilderUtils.getPatientBirthColumn(criterionId))))
      } else {
        dataFrame.withColumn(
          colName = QueryColumn.AGE,
          F.datediff(F.col(s"${QueryBuilderUtils.getDateColumn(criterionId)}"),
                     F.col(QueryBuilderUtils.getPatientBirthColumn(criterionId))) / 365.25
        )
      }
    }

    def getAgeFilter(patientAge: PatientAge, dateIsNotNull: Boolean): Column = {
      val (year_min, month_min, day_min, year_max, month_max, day_max) =
        getDecomposedAgeMinAndMax(patientAge)
      var sparkFilterList = new ListBuffer[Column]()
      sparkFilterList = if (year_max == 0 & year_min == 0) {
        if (patientAge.maxAge.isDefined) {
          sparkFilterList += F.col(QueryColumn.AGE) <= month_max * 30 + day_max
        }
        if (patientAge.minAge.isDefined) {
          sparkFilterList += F.col(QueryColumn.AGE) >= month_min * 30 + day_min
        }
        sparkFilterList
      } else {
        if (patientAge.maxAge.isDefined) {
          sparkFilterList += F.col(QueryColumn.AGE) <= year_max
        }
        if (patientAge.minAge.isDefined) {
          sparkFilterList += F.col(QueryColumn.AGE) >= year_min
        }
        sparkFilterList
      }
      val unifiedSparkFilter = sparkFilterList.toList.reduce(_ && _)
      if (!dateIsNotNull)
        unifiedSparkFilter || F.col(QueryBuilderUtils.getDateColumn(criterionId)).isNull
      else unifiedSparkFilter
    }

    // check patient age at the date of the resource / WARNING : what date to choose : encounter start date is forced now
    if (basicResource.patientAge.isDefined) {
      val patientAge = basicResource.patientAge.get

      val datePreference =
        patientAge.datePreference.getOrElse(
          QueryBuilderUtils.defaultDatePreferencePerCollection(basicResource.resourceType))
      val dateIsNotNull = patientAge.dateIsNotNull.getOrElse(true)

      val criterionDataFrameWithDateColumn =
        qbUtils.buildLocalDateColumn(criterionDataFrame,
                                     criterionId,
                                     datePreference,
                                     basicResource.resourceType)
      val criterionDataFrameWithAgeColumn: DataFrame =
        addAgeColumn(criterionDataFrameWithDateColumn, patientAge)

      val ageBasedSparkFilter: Column = getAgeFilter(patientAge, dateIsNotNull)

      if (logger.isDebugEnabled)
        logger.debug(
          s"Basic Resource : filterByPatientAge : filter=$ageBasedSparkFilter, df.head=${criterionDataFrameWithDateColumn.head(10).toList.slice(0, 10)}")
      var filteredCriterionDataFrame =
        criterionDataFrameWithAgeColumn.filter(ageBasedSparkFilter === true)
      filteredCriterionDataFrame = dropTemporaryAgeColumns(filteredCriterionDataFrame)
      if (logger.isDebugEnabled)
        logger.debug(
          s"Basic Resource with _id=$criterionId  : filterByPatientAge : df_output.count=${filteredCriterionDataFrame
            .count()}")
      filteredCriterionDataFrame
    } else criterionDataFrame
  }

  /** Filter patient of input dataframe which does not have the required amount of occurrence.
    *
    * @param criterionDataFrame resulting dataframe of patient of a basicResource
    * @param basicResource      basicResource object
    * @param criterionId        id of the basicResource
    * */
  private def filterByOccurrenceNumber(criterionDataFrame: DataFrame,
                                       basicResource: BasicResource,
                                       criterionId: Short,
                                       isInTemporalConstraint: Boolean): DataFrame = {
    val sameDayColumn: String = s"${QueryBuilderUtils.getDateColumn(criterionId)}Day"

    def addSameDayConstraintColumns(dataframe: DataFrame, sameDay: Boolean): DataFrame = {
      if (sameDay) {
        // @todo : datePreference is not an option here
        val datePreference =
          QueryBuilderUtils.defaultDatePreferencePerCollection(basicResource.resourceType)
        val dataFrameWithSameDayColumn =
          qbUtils
            .buildLocalDateColumn(dataframe,
                                  criterionId,
                                  datePreference,
                                  basicResource.resourceType)
            .withColumn(sameDayColumn,
                        F.to_date(F.col(QueryBuilderUtils.getDateColumn(criterionId))))
        dataFrameWithSameDayColumn
      } else dataframe
    }

    def addSameDayGroupByColumns(sameDay: Boolean,
                                 groupByColumns: ListBuffer[String]): ListBuffer[String] = {
      if (sameDay) groupByColumns += sameDayColumn
      else groupByColumns
    }

    def addSameEncounterGroupByColumns(sameEncounter: Boolean,
                                       groupByColumns: ListBuffer[String]): ListBuffer[String] = {
      if (sameEncounter) {
        groupByColumns += QueryBuilderUtils.getEncounterColumn(criterionId)
      } else groupByColumns
    }

    def getPatientListFilteredByOccurrenceNumber(dataframe: DataFrame,
                                                 groupByColumns: ListBuffer[String],
                                                 operator: String,
                                                 n: Int) = {

      if (logger.isInfoEnabled)
        logger.info(s"*** GroupByColumns: $groupByColumns  FILTER: 'count $operator $n'")

      dataframe
        .groupBy(groupByColumns.head, groupByColumns.tail.toList: _*)
        .count()
        .filter(s"count $operator $n")
        .drop("count")
    }

    def getResultingDataFrame(criterionDataFrame: DataFrame,
                              patientListDataFrame: DataFrame,
                              sameDay: Boolean,
                              sameEncounter: Boolean,
                              groupByColumns: ListBuffer[String]) = {
      if (isInTemporalConstraint && (sameDay || sameEncounter)) {
        var renamedPatientListDataFrame = patientListDataFrame
        for (x <- groupByColumns) {
          renamedPatientListDataFrame =
            renamedPatientListDataFrame.withColumnRenamed(x, x.split('_').last)
        }
        val joinExprs = groupByColumns
          .map(x => criterionDataFrame(x) <=> renamedPatientListDataFrame(x.split('_').last))
          .reduce(_ && _)
        criterionDataFrame
          .join(renamedPatientListDataFrame, joinExprs, "right")
          .drop(groupByColumns.map(x => x.split('_').last).toList: _*)
      } else {
        val joinId: String = QueryBuilderUtils.getSubjectColumn(criterionId)
        criterionDataFrame.join(patientListDataFrame,
                                criterionDataFrame(joinId) <=> patientListDataFrame(joinId),
                                "left_semi")
      }
    }

    if (basicResource.occurrence.isDefined) {
      val n = basicResource.occurrence.get.n
      var operator = basicResource.occurrence.get.operator
      if (logger.isInfoEnabled)
        logger.info(s"Operator : $operator Occurrence : $n")
      if (operator != ">=" || n != 1) {
        val sameDay = basicResource.occurrence.get.sameDay.getOrElse(false)
        val sameEncounter = basicResource.occurrence.get.sameEncounter.getOrElse(false)

        operator = if (operator == "=") "==" else operator

        var groupByColumns = ListBuffer[String](QueryBuilderUtils.getSubjectColumn(criterionId))
        val criterionDataFrameWithSameDayColumn: DataFrame =
          addSameDayConstraintColumns(criterionDataFrame, sameDay)

        groupByColumns = addSameDayGroupByColumns(sameDay, groupByColumns)
        groupByColumns = addSameEncounterGroupByColumns(sameEncounter, groupByColumns)
        if (logger.isInfoEnabled)
          logger.info(s"Basic Resource Occurrence group_by cols : $groupByColumns")
        if (logger.isInfoEnabled)
          logger.info(s"DF EXAMPLE : ${criterionDataFrameWithSameDayColumn.columns.toSeq}")
        val filterPatientDataFrame: DataFrame =
          getPatientListFilteredByOccurrenceNumber(criterionDataFrameWithSameDayColumn,
                                                   groupByColumns,
                                                   operator,
                                                   n)
        val resultDataFrame: DataFrame = getResultingDataFrame(criterionDataFrame,
                                                               filterPatientDataFrame,
                                                               sameDay,
                                                               sameEncounter,
                                                               groupByColumns)
        if (logger.isDebugEnabled)
          logger.debug(
            s"Basic Resource with _id=$criterionId and columns=${resultDataFrame.columns.toSeq} : filterByOccurrenceNumber : df_output.count=${resultDataFrame.count}")
        resultDataFrame
      } else criterionDataFrame
    } else criterionDataFrame
  }

  def processFhirRessource(implicit spark: SparkSession,
                           sourcePopulation: SourcePopulation,
                           criterionTags: CriterionTags,
                           basicResource: BasicResource): DataFrame = {
    val criterionId: Short = basicResource._id
    val isInTemporalConstraint: Boolean = criterionTags.isInTemporalConstraint
    val subjectColumn =
      QueryBuilderUtils.getSubjectColumn(criterionId, isPatient = !criterionTags.isResourceFilter)
    val selectedColumns = List(subjectColumn) ++ (if (criterionTags.withOrganizations)
                                                    List(
                                                      QueryBuilderUtils
                                                        .getOrganizationsColumn(criterionId))
                                                  else
                                                    List())
    // Resolver request
    var criterionDataFrame: DataFrame =
      querySolver.getResourceDataFrame(basicResource, criterionTags, sourcePopulation)
    // set column names with prepended criterionId
    criterionDataFrame = criterionDataFrame.toDF(
      criterionDataFrame.columns.map(c => QueryBuilderUtils.buildColName(criterionId, c)).toSeq: _*)
    if (logger.isDebugEnabled) {
      logger.debug(
        s"criterionDataFrame recovered, columns are: ${criterionDataFrame.columns.mkString("Array(", ", ", ")")}")
    }

    // Apply advanced parameters
    criterionDataFrame = filterByDateRangeList(criterionDataFrame, basicResource, criterionId)
    criterionDataFrame = filterByPatientAge(criterionDataFrame, basicResource, criterionId)
    criterionDataFrame = filterByOccurrenceNumber(criterionDataFrame,
                                                  basicResource,
                                                  criterionId,
                                                  isInTemporalConstraint)
    criterionDataFrame = qbUtils.cleanDataFrame(criterionDataFrame,
                                                isInTemporalConstraint,
                                                selectedColumns,
                                                subjectColumn)

    if (logger.isDebugEnabled) {
      logger.debug(
        s"Basic Resource with _id=$criterionId : final criterionDataFrame : " +
          s"criterionDataFrame.count=${criterionDataFrame.count}, " +
          s"criterionDataFrame.columns=${criterionDataFrame.columns.toList}, " +
          s"criterionDataFrame.head=${criterionDataFrame.head(10).toList.slice(0, 10)}")
    }
    criterionDataFrame
  }

}
