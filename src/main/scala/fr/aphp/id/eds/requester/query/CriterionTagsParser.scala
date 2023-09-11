package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.QueryColumn.EVENT_DATE
import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.{DATE_COL, ENCOUNTER_COL, ENCOUNTER_DATES_COL, ENCOUNTER_ID, SolrCollection, SolrColumn}
import fr.aphp.id.eds.requester.query.QueryParser.{DataValueShortList, DataValueString, GenericQuery, GenericTemporalConstraint}
import org.apache.log4j.Logger

/** Tags for each criterion.
 *
 * @param isDateTimeAvailable  tells if the criterion contains datetime information
 * @param isEncounterAvailable tells if the criterion contains encounter information
 * @param resourceType         criterion resource name - 'default' for groups
 */
class CriterionTags(val isDateTimeAvailable: Boolean,
                    val isEncounterAvailable: Boolean,
                    val isInTemporalConstraint: Boolean,
                    val temporalConstraintTypeList: List[String] = List[String](),
                    val resourceType: String = "default",
                    val requiredSolrFieldList: List[String] = List[String](),
                    val isResourceFilter: Boolean = false,
                   )

object CriterionTagsParser {
  private val logger = Logger.getLogger(this.getClass)
  private val queryBuilderConfigs = new QueryBuilderConfigs()

  /** Extracts a map that links all criteria id concerned by a temporal constraint with the required dataPreference list.
   *
   * @param genericQuery the request query
   */
  def getCriterionTagsMap(
                           genericQuery: GenericQuery,
                           criterionTagsMap: Map[Short, CriterionTags],
                           inheritedTemporalConstraintList: List[GenericTemporalConstraint] =
                           List[GenericTemporalConstraint](),
                           isResourceFilter: Boolean = false
                         ): Map[Short, CriterionTags] = {

    val resultingCriterionTagsMap: Map[Short, CriterionTags] = genericQuery._type match {
      case "request" =>
        if (genericQuery.request.isEmpty)
          criterionTagsMap
        else {
          getCriterionTagsMap(genericQuery.request.get, criterionTagsMap, List(), genericQuery.resourceType.getOrElse(ResourceType.patient) != ResourceType.patient)
        }

      case "basicResource" =>
        val criterionId = genericQuery._id.get
        val criterionTags = getBasicRessourceTags(genericQuery, isResourceFilter)
        val criterionTagsMapTmp: Map[Short, CriterionTags] =
          Map[Short, CriterionTags](criterionId -> criterionTags)
        mergeCriterionTagsMap(criterionTagsMap, criterionTagsMapTmp)

      case _ => // if group
        var criterionTagsMapTmp = criterionTagsMap
        val criteria = genericQuery.criteria.getOrElse(List[GenericQuery]())
        var temporalConstraints =
          genericQuery.temporalConstraints.getOrElse(List[GenericTemporalConstraint]())
        temporalConstraints ++= inheritedTemporalConstraintList.map(
          tc =>
            GenericTemporalConstraint(
              DataValueString("all"),
              tc.constraintType,
              tc.occurrenceChoiceList,
              tc.timeRelationMinDuration,
              tc.timeRelationMaxDuration,
              tc.datePreferenceList,
              tc.filteredCriteriaIdList,
              tc.dateIsNotNullList
            ))
        for (criterion <- criteria) {
          criterionTagsMapTmp = mergeCriterionTagsMap(
            criterionTagsMapTmp,
            getCriterionTagsMap(criterion, criterionTagsMapTmp, temporalConstraints)
          )
        }
        criterionTagsMapTmp = mergeCriterionTagsMap(
          criterionTagsMapTmp,
          processTemporalConstraintToGetTagsPerId(genericQuery,
            temporalConstraints,
            criterionTagsMapTmp)
        )
        criterionTagsMapTmp = mergeCriterionTagsMap(
          criterionTagsMapTmp,
          Map(genericQuery._id.get -> getGroupTags(criteria, criterionTagsMapTmp)))
        criterionTagsMapTmp
    }
    if (logger.isDebugEnabled)
      logger.debug(resultingCriterionTagsMap
        .map(x =>
          "AVAILABLE CRITERION_TAGS_MAP: " + s"${x._1}: ${x._2.isEncounterAvailable}, ${x._2.isDateTimeAvailable}, ${x._2.isInTemporalConstraint}, ${x._2.requiredSolrFieldList}, ${x._2.resourceType}")
        .mkString(" || "))
    resultingCriterionTagsMap
  }

  /** Increment the map with criteria concerned by temporal constraint of a group
   *
   * @param genericQuery group of criteria */
  private def processTemporalConstraintToGetTagsPerId(
                                                       genericQuery: GenericQuery,
                                                       temporalConstraints: List[GenericTemporalConstraint],
                                                       criterionTagsMap: Map[Short, CriterionTags]
                                                     ): Map[Short, CriterionTags] = {

    def getConcernedCriteriaByTemporalConstraint(
                                                  temporalConstraint: GenericTemporalConstraint,
                                                  criteria: List[GenericQuery]): List[GenericQuery] = {
      temporalConstraint.idList match {
        case DataValueString(_) => criteria.filter(x => x.isInclusive.get)
        case DataValueShortList(idList) =>
          criteria.filter(x => idList.contains(x._id.get))
      }
    }

    def getCriterionTemporalConstraintDatePreference(isTemporalConstraintAboutDateTime: Boolean,
                                                     temporalConstraint: GenericTemporalConstraint,
                                                     collection: String,
                                                     criterion: GenericQuery): List[String] = {
      val normalizedDatePreferenceList: List[String] =
        if (collection == SolrCollection.DEFAULT) List[String]()
        else if (!isTemporalConstraintAboutDateTime) List(ENCOUNTER_ID)
        else {
          if (temporalConstraint.datePreferenceList.isDefined) {
            val localDatePreference =
              temporalConstraint.datePreferenceList.get.filter(dp => dp.i == criterion._id.get)
            if (localDatePreference.isEmpty)
              queryBuilderConfigs.defaultDatePreferencePerCollection(collection)
            else localDatePreference.head.datePreference
          } else queryBuilderConfigs.defaultDatePreferencePerCollection(collection)
        }
      convertDatePreferenceToDateTimeSolrField(normalizedDatePreferenceList, collection)
    }

    var criterionTagsMapTmp = criterionTagsMap
    val criteria = genericQuery.criteria.getOrElse(List[GenericQuery]())
    temporalConstraints.foreach(
      temporalConstraint => {

        val constraintType = temporalConstraint.constraintType

        val isTemporalConstraintAboutDateTime: Boolean =
          List[String]("directChronologicalOrdering").contains(constraintType)
        val isTemporalConstraintAboutEncounter: Boolean =
          List[String]("sameEncounter", "differentEncounter").contains(constraintType)

        val concernedCriteria = getConcernedCriteriaByTemporalConstraint(temporalConstraint, criteria)

        concernedCriteria.foreach(
          criterion => {
            val criterionId = criterion._id.get
            val collection: String = criterionTagsMapTmp(criterionId).resourceType
            var requiredSolrFieldList: List[String] =
              criterionTagsMapTmp(criterionId).requiredSolrFieldList
            val isDateTimeAvailable: Boolean = criterionTagsMapTmp(criterionId).isDateTimeAvailable
            val isEncounterAvailable: Boolean =
              criterionTagsMapTmp(criterionId).isEncounterAvailable
            var constraintTypeList: List[String] =
              criterionTagsMapTmp(criterionId).temporalConstraintTypeList

            val isInTemporalConstraint: Boolean = (isEncounterAvailable && isTemporalConstraintAboutEncounter) || (isDateTimeAvailable && isTemporalConstraintAboutDateTime)

            constraintTypeList = if (isInTemporalConstraint) constraintTypeList ++ List(constraintType) else constraintTypeList

            requiredSolrFieldList = getCriterionTemporalConstraintDatePreference(isTemporalConstraintAboutDateTime,
              temporalConstraint,
              collection,
              criterion) ++ requiredSolrFieldList

            val criterionTags = new CriterionTags(
              isDateTimeAvailable,
              isEncounterAvailable,
              isInTemporalConstraint,
              constraintTypeList,
              collection,
              requiredSolrFieldList
            )
            criterionTagsMapTmp = mergeCriterionTagsMap(criterionTagsMapTmp, Map(criterionId -> criterionTags))
          }
        )
      }
    )
    criterionTagsMapTmp
  }

  private def mergeCriterionTagsMap(map1: Map[Short, CriterionTags],
                                    map2: Map[Short, CriterionTags]): Map[Short, CriterionTags] = {
    var mergedMap = map1
    map2.foreach(i => {
      mergedMap =
        if (mergedMap.contains(i._1))
          mergedMap + (i._1 -> new CriterionTags(
            mergedMap(i._1).isDateTimeAvailable || i._2.isDateTimeAvailable,
            mergedMap(i._1).isEncounterAvailable || i._2.isEncounterAvailable,
            mergedMap(i._1).isInTemporalConstraint || i._2.isInTemporalConstraint,
            (mergedMap(i._1).temporalConstraintTypeList ++ i._2.temporalConstraintTypeList).distinct,
            mergedMap(i._1).resourceType,
            (mergedMap(i._1).requiredSolrFieldList ++ i._2.requiredSolrFieldList).distinct,
            mergedMap(i._1).isResourceFilter || i._2.isResourceFilter
          ))
        else mergedMap + (i._1 -> i._2)
    })
    mergedMap
  }

  private def getBasicRessourceTags(genericQuery: GenericQuery, isResourceFilter: Boolean): CriterionTags = {
    var requiredSolrFieldList: List[String] = if (isResourceFilter) List[String](SolrColumn.ID) else List[String]()
    val collection = genericQuery.resourceType.get

    def getEncounterDateRangeDatetimePreferenceList(dateTimeList: List[String]): List[String] = {
      if (genericQuery.encounterDateRange.isDefined)
        (dateTimeList ++ queryBuilderConfigs
          .requestKeyPerCollectionMap(collection)
          .getOrElse(ENCOUNTER_DATES_COL, List[String]())).distinct
      else dateTimeList
    }

    def getDateRangeDatetimePreferenceList(dateTimeList: List[String]): List[String] = {
      var dateTimeListTmp = dateTimeList
      if (genericQuery.dateRangeList.isDefined) {
        genericQuery.dateRangeList.get.foreach(
          dateRange =>
            dateTimeListTmp =
              if (dateRange.datePreference.isDefined)
                dateTimeListTmp ++ dateRange.datePreference.get
              else
                dateTimeListTmp ++ queryBuilderConfigs.defaultDatePreferencePerCollection(
                  collection)
        )
        dateTimeListTmp.distinct
      } else dateTimeList
    }

    def getPatientAgeDatetimePreferenceList(dateTimeList: List[String]): List[String] = {
      if (genericQuery.patientAge.isDefined) {
        if (genericQuery.patientAge.get.datePreference.isDefined)
          (dateTimeList ++ genericQuery.patientAge.get.datePreference.get).distinct
        else
          (dateTimeList ++ queryBuilderConfigs.defaultDatePreferencePerCollection(collection)).distinct
      } else dateTimeList
    }

    def getSameDayOccurrenceDatetimePreferenceList(dateTimeList: List[String]): List[String] = {
      if (genericQuery.occurrence.isDefined && genericQuery.occurrence.get.sameDay.isDefined && genericQuery.occurrence.get.sameDay.get) {
        (dateTimeList ++ queryBuilderConfigs.defaultDatePreferencePerCollection(collection)).distinct
      } else dateTimeList
    }

    def getSameEncounterOccurrenceFieldList(solrFieldList: List[String]): List[String] = {
      if (genericQuery.occurrence.isDefined && genericQuery.occurrence.get.sameEncounter.isDefined && genericQuery.occurrence.get.sameEncounter.get) {
        (solrFieldList ++ List(ENCOUNTER_ID)).distinct
      } else solrFieldList
    }

    def getIsDateTimeAvailable: Boolean = {
      queryBuilderConfigs
        .requestKeyPerCollectionMap(collection)
        .contains(DATE_COL) || queryBuilderConfigs
        .requestKeyPerCollectionMap(collection)
        .contains(ENCOUNTER_DATES_COL)
    }

    def getIsEncounterAvailable: Boolean = {
      queryBuilderConfigs
        .requestKeyPerCollectionMap(collection)
        .contains(ENCOUNTER_COL)
    }

    requiredSolrFieldList = getEncounterDateRangeDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getDateRangeDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getPatientAgeDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getSameDayOccurrenceDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getSameEncounterOccurrenceFieldList(requiredSolrFieldList)
    requiredSolrFieldList = convertDatePreferenceToDateTimeSolrField(requiredSolrFieldList, collection)
    val isDateTimeAvailable: Boolean = getIsDateTimeAvailable
    val isEncounterAvailable: Boolean = getIsEncounterAvailable
    new CriterionTags(isDateTimeAvailable, isEncounterAvailable, false, List[String](), collection, requiredSolrFieldList, isResourceFilter)
  }

  private def getGroupTags(criteria: List[GenericQuery], criterionTagsMap: Map[Short, CriterionTags]): CriterionTags = {

    val inclusionCriteriaIdList: List[Short] = criteria.filter(x => x.isInclusive.get).map(x => x._id.get)

    def getIsDateTimeAvailable: Boolean = {
      inclusionCriteriaIdList
        .map(x => criterionTagsMap(x).isDateTimeAvailable)
        .reduceOption(_ || _)
        .getOrElse(false)
    }

    def getIsEncounterAvailable: Boolean = {
      inclusionCriteriaIdList
        .map(x => criterionTagsMap(x).isEncounterAvailable)
        .reduceOption(_ || _)
        .getOrElse(false)
    }

    val isDateTimeAvailable: Boolean = getIsDateTimeAvailable
    val isEncounterAvailable: Boolean = getIsEncounterAvailable
    new CriterionTags(isDateTimeAvailable, isEncounterAvailable, false)
  }

  private def convertDatePreferenceToDateTimeSolrField(datePreferenceList: List[String],
                                                       collection: String): List[String] = {
    var dateTimeSolrFieldList: List[String] = List[String]()
    val translationMap = queryBuilderConfigs.requestKeyPerCollectionMap
      .getOrElse(collection, Map[String, List[String]]())
    datePreferenceList.foreach {
      case EVENT_DATE => dateTimeSolrFieldList ++= translationMap.getOrElse(DATE_COL, List[String]())
      case ENCOUNTER_ID => dateTimeSolrFieldList ++= translationMap.getOrElse(ENCOUNTER_COL, List[String]())
      case SolrColumn.ENCOUNTER_START_DATE | SolrColumn.ENCOUNTER_END_DATE =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(ENCOUNTER_DATES_COL, List[String]())
      case alreadyGoodString: String => dateTimeSolrFieldList ::= alreadyGoodString
    }
    dateTimeSolrFieldList.distinct
  }

}
