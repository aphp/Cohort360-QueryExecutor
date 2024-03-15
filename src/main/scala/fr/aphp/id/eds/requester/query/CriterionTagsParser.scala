package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.QueryColumn.{
  ENCOUNTER_END_DATE,
  ENCOUNTER_START_DATE,
  EPISODE_OF_CARE,
  EVENT_DATE
}
import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.query.QueryParser.{
  DataValueShortList,
  DataValueString,
  GenericQuery,
  GenericTemporalConstraint
}
import fr.aphp.id.eds.requester.query.TemporalConstraintType.{
  DIFFERENT_ENCOUNTER,
  DIRECT_CHRONOLOGICAL_ORDERING,
  SAME_ENCOUNTER,
  SAME_EPISODE_OF_CARE
}
import fr.aphp.id.eds.requester._
import org.apache.log4j.Logger

/** Tags for each criterion.
  *
  * @param isDateTimeAvailable  tells if the criterion contains datetime information
  * @param isEncounterAvailable tells if the criterion contains encounter information
  * @param resourceType         criterion resource name - 'default' for groups
  */
class CriterionTags(val isDateTimeAvailable: Boolean,
                    val isEncounterAvailable: Boolean,
                    val isEpisodeOfCareAvailable: Boolean,
                    val isInTemporalConstraint: Boolean,
                    val temporalConstraintTypeList: List[String] = List[String](),
                    val resourceType: String = "default",
                    val requiredSolrFieldList: List[String] = List[String](),
                    val isResourceFilter: Boolean = false,
                    val withOrganizations: Boolean = false,
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
      isResourceFilter: Boolean = false,
      requestOrganizations: Boolean = false
  ): Map[Short, CriterionTags] = {

    val resultingCriterionTagsMap: Map[Short, CriterionTags] = genericQuery._type match {
      case "request" =>
        if (genericQuery.request.isEmpty)
          criterionTagsMap
        else {
          getCriterionTagsMap(
            genericQuery.request.get,
            criterionTagsMap,
            List(),
            genericQuery.resourceType.getOrElse(ResourceType.patient) != ResourceType.patient,
            requestOrganizations)
        }

      case "basicResource" =>
        val criterionId = genericQuery._id.get
        val criterionTags =
          getBasicRessourceTags(genericQuery, isResourceFilter, requestOrganizations)
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
            getCriterionTagsMap(criterion,
                                criterionTagsMapTmp,
                                temporalConstraints,
                                isResourceFilter,
                                requestOrganizations)
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
          Map(
            genericQuery._id.get -> getGroupTags(criteria,
                                                 criterionTagsMapTmp,
                                                 requestOrganizations)))
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
      var normalizedDatePreferenceList: List[String] =
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
      normalizedDatePreferenceList = normalizedDatePreferenceList ++ (if (temporalConstraint.constraintType == SAME_EPISODE_OF_CARE) {
                                                                        List(EPISODE_OF_CARE_COL)
                                                                      } else {
                                                                        List()
                                                                      })
      convertDatePreferenceToDateTimeSolrField(normalizedDatePreferenceList, collection)
    }

    var criterionTagsMapTmp = criterionTagsMap
    val criteria = genericQuery.criteria.getOrElse(List[GenericQuery]())
    temporalConstraints.foreach(
      temporalConstraint => {

        val constraintType = temporalConstraint.constraintType

        val isTemporalConstraintAboutDateTime: Boolean =
          List[String](DIRECT_CHRONOLOGICAL_ORDERING).contains(constraintType)
        val isTemporalConstraintAboutEncounter: Boolean =
          List[String](SAME_ENCOUNTER, DIFFERENT_ENCOUNTER).contains(constraintType)
        val isTemporalConstraintAboutEpisodeOfCare: Boolean =
          List[String](SAME_EPISODE_OF_CARE).contains(constraintType)

        val concernedCriteria =
          getConcernedCriteriaByTemporalConstraint(temporalConstraint, criteria)

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

            val isInTemporalConstraint
              : Boolean = (isEncounterAvailable && isTemporalConstraintAboutEncounter) || (isDateTimeAvailable && isTemporalConstraintAboutDateTime) || (isTemporalConstraintAboutEpisodeOfCare)

            constraintTypeList =
              if (isInTemporalConstraint) constraintTypeList ++ List(constraintType)
              else constraintTypeList

            requiredSolrFieldList = getCriterionTemporalConstraintDatePreference(
              isTemporalConstraintAboutDateTime,
              temporalConstraint,
              collection,
              criterion) ++ requiredSolrFieldList

            val criterionTags = new CriterionTags(
              isDateTimeAvailable,
              isEncounterAvailable,
              criterionTagsMapTmp(criterionId).isEpisodeOfCareAvailable,
              isInTemporalConstraint,
              constraintTypeList,
              collection,
              requiredSolrFieldList
            )
            criterionTagsMapTmp =
              mergeCriterionTagsMap(criterionTagsMapTmp, Map(criterionId -> criterionTags))
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
            mergedMap(i._1).isEpisodeOfCareAvailable || i._2.isEpisodeOfCareAvailable,
            mergedMap(i._1).isInTemporalConstraint || i._2.isInTemporalConstraint,
            (mergedMap(i._1).temporalConstraintTypeList ++ i._2.temporalConstraintTypeList).distinct,
            mergedMap(i._1).resourceType,
            (mergedMap(i._1).requiredSolrFieldList ++ i._2.requiredSolrFieldList).distinct,
            mergedMap(i._1).isResourceFilter || i._2.isResourceFilter,
            mergedMap(i._1).withOrganizations || i._2.withOrganizations,
          ))
        else mergedMap + (i._1 -> i._2)
    })
    mergedMap
  }

  private def getBasicRessourceTags(genericQuery: GenericQuery,
                                    isResourceFilter: Boolean,
                                    requestOrganization: Boolean): CriterionTags = {
    var requiredSolrFieldList =
      Map(
        isResourceFilter -> SolrColumn.ID,
        requestOrganization -> SolrColumn.ORGANIZATIONS
      ).foldLeft(List[String]()) {
        case (acc, (key, value)) =>
          if (key) value :: acc else acc
      }
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

    def getResourceGroupByFieldOccurrenceFieldList(solrFieldList: List[String]): List[String] = {
      if (genericQuery.occurrence.isDefined
        && (genericQuery.occurrence.get.n != 1 || genericQuery.occurrence.get.operator != ">=")
        && queryBuilderConfigs.requestKeyPerCollectionMap(collection).contains(GROUP_BY_COLUMN)
      ) {
        (solrFieldList ++ queryBuilderConfigs.requestKeyPerCollectionMap(collection)(GROUP_BY_COLUMN)).distinct
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

    def getIsEpisodeOfCareAvailable: Boolean = {
      queryBuilderConfigs
        .requestKeyPerCollectionMap(collection)
        .contains(EPISODE_OF_CARE_COL)
    }

    requiredSolrFieldList = getEncounterDateRangeDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getDateRangeDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getPatientAgeDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getSameDayOccurrenceDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getSameEncounterOccurrenceFieldList(requiredSolrFieldList)
    requiredSolrFieldList = getResourceGroupByFieldOccurrenceFieldList(requiredSolrFieldList)
    requiredSolrFieldList =
      convertDatePreferenceToDateTimeSolrField(requiredSolrFieldList, collection)
    val isDateTimeAvailable: Boolean = getIsDateTimeAvailable
    val isEncounterAvailable: Boolean = getIsEncounterAvailable
    val isEpisodeOfCareAvailable: Boolean = getIsEpisodeOfCareAvailable
    new CriterionTags(isDateTimeAvailable,
                      isEncounterAvailable,
                      isEpisodeOfCareAvailable,
                      false,
                      List[String](),
                      collection,
                      requiredSolrFieldList,
                      isResourceFilter,
                      requestOrganization)
  }

  private def getGroupTags(criteria: List[GenericQuery],
                           criterionTagsMap: Map[Short, CriterionTags],
                           requestOrganization: Boolean): CriterionTags = {

    val inclusionCriteriaIdList: List[Short] =
      criteria.filter(x => x.isInclusive.get).map(x => x._id.get)

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

    def getIsEpisodeOfCareAvailable: Boolean = {
      inclusionCriteriaIdList
        .map(x => criterionTagsMap(x).isEpisodeOfCareAvailable)
        .reduceOption(_ || _)
        .getOrElse(false)
    }

    val isDateTimeAvailable: Boolean = getIsDateTimeAvailable
    val isEncounterAvailable: Boolean = getIsEncounterAvailable
    new CriterionTags(isDateTimeAvailable,
                      isEncounterAvailable,
                      getIsEpisodeOfCareAvailable,
                      false,
                      withOrganizations = requestOrganization)
  }

  private def convertDatePreferenceToDateTimeSolrField(datePreferenceList: List[String],
                                                       collection: String): List[String] = {
    var dateTimeSolrFieldList: List[String] = List[String]()
    val translationMap = queryBuilderConfigs.requestKeyPerCollectionMap
      .getOrElse(collection, Map[String, List[String]]())
    datePreferenceList.foreach {
      case EVENT_DATE =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(DATE_COL, List[String]())
      case ENCOUNTER_ID =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(ENCOUNTER_COL, List[String]())
      case ENCOUNTER_START_DATE | ENCOUNTER_END_DATE =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(ENCOUNTER_DATES_COL, List[String]())
      case EPISODE_OF_CARE_COL =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(EPISODE_OF_CARE_COL, List[String]())
      case alreadyGoodString: String => dateTimeSolrFieldList ::= alreadyGoodString
    }
    dateTimeSolrFieldList.distinct
  }

}
