package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.QueryColumn.EVENT_DATE
import fr.aphp.id.eds.requester.tools.JobUtils.getDefaultSolrFilterQuery
import fr.aphp.id.eds.requester.tools.SolrTools.getSolrClient
import fr.aphp.id.eds.requester._
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.{SolrQuery, SolrRequest}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}

import scala.collection.mutable.ListBuffer

class QueryBuilderBasicResource(val qbConfigs: QueryBuilderConfigs = new QueryBuilderConfigs(),
                                val qbUtils: QueryBuilderUtils = new QueryBuilderUtils(),
                                val querySolver: SolrQueryResolver = SolrQueryResolver) {
  val requestKeyPerCollectionMap: Map[String, Map[String, List[String]]] =
    qbConfigs.requestKeyPerCollectionMap

  private val logger = Logger.getLogger(this.getClass)

  /** Changing names of columns so that there is no difference between resources in the name of columns. */
  private def homogenizeColumns(collectionName: String,
                                df: DataFrame,
                                localId: Short): DataFrame = {

    /** Homogenizes the column names across collections.
      *
      * @param collection              the collection name of the ressource
      * @param requestKeyPerCollection the map storing the name of columns per category */
    def homogenizeDatetimeColumnNames(
        collection: String,
        requestKeyPerCollection: Map[String, Map[String, List[String]]] = requestKeyPerCollectionMap)
      : String => String =
      (column_name: String) => {
        val dateField =
          requestKeyPerCollection(collection).getOrElse(DATE_COL, List[String]("")).head
        val patientField =
          requestKeyPerCollection(collection).getOrElse(PATIENT_COL, List[String]("")).head
        val encounterField =
          requestKeyPerCollection(collection).getOrElse(ENCOUNTER_COL, List("")).head
        collection match {
          case SolrCollection.ENCOUNTER_APHP =>
            column_name match {
              case SolrColumn.Encounter.PERIOD_START => QueryColumn.ENCOUNTER_START_DATE
              case SolrColumn.Encounter.PERIOD_END   => QueryColumn.ENCOUNTER_END_DATE
              case SolrColumn.PATIENT_BIRTHDATE      => QueryColumn.PATIENT_BIRTHDATE
              case SolrColumn.ORGANIZATIONS          => QueryColumn.ORGANIZATIONS
              case `dateField`                       => EVENT_DATE
              case `patientField`                    => QueryColumn.PATIENT
              case `encounterField`                  => QueryColumn.ENCOUNTER
              case _                                 => column_name.replace(".", "_")
            }
          case SolrCollection.PATIENT_APHP =>
            column_name match {
              case SolrColumn.Patient.BIRTHDATE => QueryColumn.PATIENT_BIRTHDATE
              case SolrColumn.ORGANIZATIONS     => QueryColumn.ORGANIZATIONS
              case `dateField`                  => QueryColumn.EVENT_DATE
              case `patientField`               => QueryColumn.PATIENT
              case _                            => column_name.replace(".", "_")
            }
          case _ =>
            column_name match {
              case `dateField`                     => QueryColumn.EVENT_DATE
              case `patientField`                  => QueryColumn.PATIENT
              case SolrColumn.PATIENT_BIRTHDATE    => QueryColumn.PATIENT_BIRTHDATE
              case SolrColumn.ENCOUNTER_START_DATE => QueryColumn.ENCOUNTER_START_DATE
              case SolrColumn.ENCOUNTER_END_DATE   => QueryColumn.ENCOUNTER_END_DATE
              case SolrColumn.ORGANIZATIONS        => QueryColumn.ORGANIZATIONS
              case `encounterField`                => QueryColumn.ENCOUNTER
              case _                               => column_name.replace(".", "_")
            }
        }
      }

    val convFunc = homogenizeDatetimeColumnNames(collectionName)
    df.toDF(df.columns.map(c => qbConfigs.buildColName(localId, convFunc(c))).toSeq: _*)

  }

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
        sparkFilterList += F.col(qbConfigs.getDateColumn(criterionId)) <= s"${dateRange.maxDate.get}"
      if (dateRange.minDate.isDefined)
        sparkFilterList += F.col(qbConfigs.getDateColumn(criterionId)) >= s"${dateRange.minDate.get}"
      val unifiedSparkFilter: Column = sparkFilterList.toList.reduce(_ && _)
      if (!dateIsNotNull) // F.col(qbConfigs.getDateColumn(criterionId)).isNull
        unifiedSparkFilter || F.col(qbConfigs.getDateColumn(criterionId)).isNull
      else unifiedSparkFilter
    }

    dateRangeList = addEncounterDateRangeToDateRangeList()

    if (dateRangeList.isDefined) {
      var filteredCriterionDataFrame: DataFrame = criterionDataFrame
      for (dateRange <- dateRangeList.get) {
        val datePreference =
          dateRange.datePreference.getOrElse(
            qbConfigs.defaultDatePreferencePerCollection(basicResource.resourceType))
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
          !List(QueryColumn.AGE, qbConfigs.getPatientBirthColumn(criterionId)).contains(c))
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
        dataFrame.withColumn(QueryColumn.AGE,
                             F.datediff(F.col(s"${qbConfigs.getDateColumn(criterionId)}"),
                                        F.col(qbConfigs.getPatientBirthColumn(criterionId))))
      } else {
        dataFrame.withColumn(
          colName = QueryColumn.AGE,
          F.datediff(F.col(s"${qbConfigs.getDateColumn(criterionId)}"),
                     F.col(qbConfigs.getPatientBirthColumn(criterionId))) / 365.25)
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
        unifiedSparkFilter || F.col(qbConfigs.getDateColumn(criterionId)).isNull
      else unifiedSparkFilter
    }

    // check patient age at the date of the resource / WARNING : what date to choose : encounter start date is forced now
    if (basicResource.patientAge.isDefined) {
      val patientAge = basicResource.patientAge.get

      val datePreference =
        patientAge.datePreference.getOrElse(
          qbConfigs.defaultDatePreferencePerCollection(basicResource.resourceType))
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
    val sameDayColumn: String = s"${qbConfigs.getDateColumn(criterionId)}Day"

    def addSameDayConstraintColumns(dataframe: DataFrame, sameDay: Boolean): DataFrame = {
      if (sameDay) {
        // @todo : datePreference is not an option here
        val datePreference =
          qbConfigs.defaultDatePreferencePerCollection(basicResource.resourceType)
        val dataFrameWithSameDayColumn =
          qbUtils
            .buildLocalDateColumn(dataframe,
                                  criterionId,
                                  datePreference,
                                  basicResource.resourceType)
            .withColumn(sameDayColumn, F.to_date(F.col(qbConfigs.getDateColumn(criterionId))))
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
        groupByColumns += qbConfigs.getEncounterColumn(criterionId)
      } else groupByColumns
    }

    def getPatientListFilteredByOccurrenceNumber(dataframe: DataFrame,
                                                 groupByColumns: ListBuffer[String],
                                                 operator: String,
                                                 n: Short) = {

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
        val joinId: String = qbConfigs.getSubjectColumn(criterionId)
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

        var groupByColumns = ListBuffer[String](qbConfigs.getSubjectColumn(criterionId))
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

  /** Get fq and fl parameters for solr based on the basic resource
    *
    * @param sourcePopulation caresite and provider source population
    * @param basicResource    the basicResource object
    */
  private def getSolrFilterQueryLegacy(sourcePopulation: SourcePopulation,
                                       basicResource: BasicResource): String = {
    def addAvailableFieldListFilterQuery(solrFilterQuery: String) = {
      val availableFieldList =
        basicResource.nullAvailableFieldList.getOrElse(List[String]())
      // TODO: Not sure of syntax and maybe to remove... always false for ipp list process
      if (availableFieldList.nonEmpty) {
        s"(($solrFilterQuery) OR (" + availableFieldList
          .map(r => s"$r:-[* TO *]")
          .mkString(",") + ")"
      } else solrFilterQuery
    }

    // @todo : unused for now
    def addEncounterDateRangeFilterQuery(solrFilterQuery: String) = {
      if (basicResource.encounterDateRange.isDefined) {
        val encounterDateRange = basicResource.encounterDateRange.get
        val encounterDateMinRequiredColumn = qbConfigs
          .requestKeyPerCollectionMap(basicResource.resourceType)(ENCOUNTER_DATES_COL)
          .head
        val encounterDateMaxRequiredColumn =
          qbConfigs.requestKeyPerCollectionMap(basicResource.resourceType)(ENCOUNTER_DATES_COL)(1)
        val listConstraint = new ListBuffer[String]()
        if (encounterDateRange.minDate.isDefined) {
          listConstraint += s"$encounterDateMinRequiredColumn:[${encounterDateRange.minDate.get} TO *]"
        }
        if (encounterDateRange.maxDate.isDefined) {
          listConstraint += s"$encounterDateMaxRequiredColumn:[* TO ${encounterDateRange.maxDate.get}]"
        }
        var encounterDateRangeFilterQuery =
          listConstraint.toList.mkString(" AND ")
        encounterDateRangeFilterQuery =
          if (encounterDateRange.dateIsNotNull.getOrElse(true))
            encounterDateRangeFilterQuery
          else
            s"($encounterDateRangeFilterQuery) OR (-$encounterDateMinRequiredColumn:[* TO *] AND -$encounterDateMaxRequiredColumn:[* TO *])"
        s"($solrFilterQuery) AND ($encounterDateRangeFilterQuery)"
      } else solrFilterQuery
    }

    def addDefaultSolrFilterQuery(solrFilterQuery: String) = {
      s"($solrFilterQuery) AND (${getDefaultSolrFilterQuery(sourcePopulation)})"
    }

    var solrFilterQuery = basicResource.filter
    solrFilterQuery = addAvailableFieldListFilterQuery(solrFilterQuery)
    // @todo : the following code is dead since encounterDateRange is treated in filterByDateRangeList to benefit from spark.coalesce
    // if not necessary (ask POs), uncomment the following because it is more efficient
    //    solrFilterQuery = addEncounterDateRangeFilterQuery(solrFilterQuery)
    addDefaultSolrFilterQuery(solrFilterQuery)
  }

  private def getSolrFilterQuery(sourcePopulation: SourcePopulation,
                                 basicResource: BasicResource): String = {
    def addDefaultCohortFqParameter(solrFilterQuery: String): String = {
      if (solrFilterQuery == null || solrFilterQuery.isEmpty) {
        return s"fq=${getDefaultSolrFilterQuery(sourcePopulation)}"
      }
      s"$solrFilterQuery&fq=${getDefaultSolrFilterQuery(sourcePopulation)}"
    }

    addDefaultCohortFqParameter(basicResource.filter)
  }

  /**
    * Determines the field names to ask for solr.
    * */
  def getSolrFilterList(criterionTags: CriterionTags, isPatientAgeConstraint: Boolean): String = {
    val collectionName: String = criterionTags.resourceType
    val fieldsPerCollectionMap = qbConfigs.requestKeyPerCollectionMap(collectionName)

    def addRequiredFields(requestedAttributes: List[String]): List[String] = {
      requestedAttributes ++ criterionTags.requiredSolrFieldList
    }

    def addPatientAgeRequiredAttributes(requestedAttributes: List[String]): List[String] = {
      if (isPatientAgeConstraint) {
        collectionName match {
          case SolrCollection.PATIENT_APHP => SolrColumn.Patient.BIRTHDATE :: requestedAttributes
          case _                           => SolrColumn.PATIENT_BIRTHDATE :: requestedAttributes
        }
      } else requestedAttributes
    }

    val requestedSolrFields = addPatientAgeRequiredAttributes(
      addRequiredFields(
        fieldsPerCollectionMap.getOrElse(PATIENT_COL, List[String]())
      )
    )
    if (logger.isDebugEnabled) {
      logger.debug(s"requested fields for $collectionName: $requestedSolrFields")
    }

    requestedSolrFields.mkString(",")
  }

  def processFhirRessource(implicit spark: SparkSession,
                           solrConf: Map[String, String],
                           sourcePopulation: SourcePopulation,
                           criterionTags: CriterionTags,
                           basicResource: BasicResource): DataFrame = {
    val criterionId: Short = basicResource._id
    val collectionName: String = basicResource.resourceType
    val isInTemporalConstraint: Boolean = criterionTags.isInTemporalConstraint
    val subjectColumn =
      qbConfigs.getSubjectColumn(criterionId, isPatient = !criterionTags.isResourceFilter)
    val selectedColumns = List(subjectColumn) ++ (if (criterionTags.withOrganizations)
                                                    List(
                                                      qbConfigs
                                                        .getOrganizationsColumn(criterionId))
                                                  else List())

    val solrFilterList = getSolrFilterList(criterionTags, basicResource.patientAge.isDefined)
    val solrFilterQuery = getSolrFilterQuery(sourcePopulation, basicResource)

    // Solr request
    var criterionDataFrame: DataFrame =
      querySolver.getSolrResponseDataFrame(basicResource.resourceType,
                                           solrFilterList,
                                           solrFilterQuery)(spark, solrConf, criterionId)
    //criterionDataFrame.write.option("header", "true").csv(s"/tmp/criterionDataFrame-${basicResource._id}")
    criterionDataFrame = homogenizeColumns(collectionName, criterionDataFrame, criterionId)
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

  /**
    * input is like "filterSolr":"(active:true) AND (identifier.value:(8006781418 8008199625 8001006675))"
    */
  def extractIdsIpp(input: String): List[String] = {
    import scala.util.matching.Regex
    val pattern: Regex = """identifier\.value:\((.*?)\)""".r
    val matches = pattern.findAllIn(input).matchData.toList
    matches.flatMap { matchData =>
      matchData.group(1).split(" ").toList
    }
  }

  def processIppList(implicit spark: SparkSession,
                     solrConf: Map[String, String],
                     sourcePopulation: SourcePopulation,
                     res: BasicResource,
                     withOrganizations: Boolean): DataFrame = {
    val solr = getSolrClient(solrConf("zkhost"))
    val localId = res._id
    val ippList: List[String] = extractIdsIpp(res.filter)
    val ippListFilter: String = ippList.mkString(",")
    // because we do not retrieve the full query translated by the CRB process
    // we need to add back this filter to the query
    val opposedSubject =
      "-(meta.security: \"http://terminology.hl7.org/CodeSystem/v3-ActCode|NOLIST\")"
    val ippFilter =
      s"({!terms f=${SolrColumn.Patient.IDENTIFIER_VALUE}}$ippListFilter) AND ${opposedSubject}"
    val ippRessource: BasicResource = BasicResource(
      localId,
      isInclusive = res.isInclusive,
      resourceType = res.resourceType,
      filter = ippFilter
    )

    val filterQuery: String = getSolrFilterQueryLegacy(sourcePopulation, ippRessource)
    val query = new SolrQuery("*:*")
      .addFilterQuery(filterQuery)
      .setStart(0)
      .setRows(100000000)
    val solrResult =
      solr.query(SolrCollection.PATIENT_APHP, query, SolrRequest.METHOD.POST).getResults
    var rdd: Seq[String] = Seq[String]()
    solrResult.forEach(doc => rdd = rdd :+ doc.get(SolrColumn.PATIENT).toString)

    solr.close()
    import spark.implicits._

    val selectedCols = List(qbConfigs.buildColName(localId, QueryColumn.PATIENT)) ++ (if (withOrganizations) {
                                                                                        List(
                                                                                          qbConfigs.buildColName(
                                                                                            localId,
                                                                                            QueryColumn.ORGANIZATIONS))
                                                                                      } else {
                                                                                        List()
                                                                                      })
    rdd.toDF(selectedCols: _*)
  }

  /** Compute the resulting df of a criteria which is a group of criteria.
    *
    * @param solrConf         solr configs extracted from SJS config
    * @param sourcePopulation caresite and provider source population
    * @param res              the basicResource object
    */
  def processRequestBasicResource(spark: SparkSession,
                                  solrConf: Map[String, String],
                                  criterionTags: CriterionTags,
                                  sourcePopulation: SourcePopulation,
                                  res: BasicResource): DataFrame = {

    if (res.resourceType.equals(IPP_LIST))
      processIppList(spark, solrConf, sourcePopulation, res, criterionTags.withOrganizations)
    else processFhirRessource(spark, solrConf, sourcePopulation, criterionTags, res)
  }
}
