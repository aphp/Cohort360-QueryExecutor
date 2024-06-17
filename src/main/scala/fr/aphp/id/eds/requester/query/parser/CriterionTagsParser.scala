package fr.aphp.id.eds.requester.query.parser

import fr.aphp.id.eds.requester.QueryColumn.{ENCOUNTER_END_DATE, ENCOUNTER_START_DATE, EVENT_DATE}
import fr.aphp.id.eds.requester._
import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.query.model.TemporalConstraintType.{DIFFERENT_ENCOUNTER, DIRECT_CHRONOLOGICAL_ORDERING, SAME_ENCOUNTER, SAME_EPISODE_OF_CARE}
import fr.aphp.id.eds.requester.query.model.{BaseQuery, BasicResource, GroupResource, Request, TemporalConstraint}
import fr.aphp.id.eds.requester.query.parser.CriterionTagsParser.queryBuilderConfigs
import fr.aphp.id.eds.requester.query.resolver.{ResourceResolverFactory, ResourceConfig}
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
                    val resourceType: String = FhirResource.UNKNOWN,
                    val requiredFieldList: List[String] = List[String](),
                    val isResourceFilter: Boolean = false,
                    val withOrganizations: Boolean = false,
)

object CriterionTagsParser {
  private val logger = Logger.getLogger(this.getClass)
  private val queryBuilderConfigs = ResourceResolverFactory.getDefaultConfig

  def getCriterionTagsMap(request: Request,
                          requestOrganizations: Boolean): Map[Short, CriterionTags] = {
    if (request.request.isEmpty)
      Map[Short, CriterionTags]()
    else
      getCriterionTagsMap(request.request.get,
                          Map[Short, CriterionTags](),
                          isResourceFilter = request.resourceType != ResourceType.patient,
                          requestOrganizations = requestOrganizations)
  }

  /** Extracts a map that links all criteria id concerned by a temporal constraint with the required dataPreference list.
    *
    * @param genericQuery the request query
    */
  def getCriterionTagsMap(
      genericQuery: BaseQuery,
      criterionTagsMap: Map[Short, CriterionTags],
      inheritedTemporalConstraintList: List[TemporalConstraint] = List[TemporalConstraint](),
      isResourceFilter: Boolean = false,
      requestOrganizations: Boolean = false
  ): Map[Short, CriterionTags] = {

    val resultingCriterionTagsMap: Map[Short, CriterionTags] = genericQuery match {
      case basicResource: BasicResource =>
        val criterionId = basicResource.i
        val criterionTags =
          getBasicRessourceTags(basicResource, isResourceFilter, requestOrganizations)
        val criterionTagsMapTmp: Map[Short, CriterionTags] =
          Map[Short, CriterionTags](criterionId -> criterionTags)
        mergeCriterionTagsMap(criterionTagsMap, criterionTagsMapTmp)

      case groupResource: GroupResource => // if group
        var criterionTagsMapTmp = criterionTagsMap
        val criteria = groupResource.criteria
        var temporalConstraints =
          groupResource.temporalConstraints.getOrElse(List[TemporalConstraint]())
        temporalConstraints ++= inheritedTemporalConstraintList.map(
          tc =>
            TemporalConstraint(
              Left("all"),
              tc.constraintType,
              tc.occurrenceChoice,
              tc.timeRelationMinDuration,
              tc.timeRelationMaxDuration,
              tc.datePreference,
              tc.dateIsNotNull,
              tc.filteredCriteriaId
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
          processTemporalConstraintToGetTagsPerId(groupResource,
                                                  temporalConstraints,
                                                  criterionTagsMapTmp)
        )
        criterionTagsMapTmp = mergeCriterionTagsMap(
          criterionTagsMapTmp,
          Map(groupResource.i -> getGroupTags(criteria, criterionTagsMapTmp, requestOrganizations)))
        criterionTagsMapTmp
    }
    if (logger.isDebugEnabled)
      logger.debug(resultingCriterionTagsMap
        .map(x =>
          "AVAILABLE CRITERION_TAGS_MAP: " + s"${x._1}: ${x._2.isEncounterAvailable}, ${x._2.isDateTimeAvailable}, ${x._2.isInTemporalConstraint}, ${x._2.requiredFieldList}, ${x._2.resourceType}")
        .mkString(" || "))
    resultingCriterionTagsMap
  }

  /** Increment the map with criteria concerned by temporal constraint of a group
    *
    * @param groupResource group of criteria */
  private def processTemporalConstraintToGetTagsPerId(
      groupResource: GroupResource,
      temporalConstraints: List[TemporalConstraint],
      criterionTagsMap: Map[Short, CriterionTags]
  ): Map[Short, CriterionTags] = {

    def getConcernedCriteriaByTemporalConstraint(temporalConstraint: TemporalConstraint,
                                                 criteria: List[BaseQuery]): List[BaseQuery] = {
      temporalConstraint.idList match {
        case Left(_) => criteria.filter(x => x.IsInclusive)
        case Right(idList) =>
          criteria.filter(x => idList.contains(x.i))
      }
    }

    def getCriterionTemporalConstraintDatePreference(isTemporalConstraintAboutDateTime: Boolean,
                                                     temporalConstraint: TemporalConstraint,
                                                     collection: String,
                                                     criterion: BaseQuery): List[String] = {
      var normalizedDatePreferenceList: List[String] =
        if (collection == FhirResource.UNKNOWN) List[String]()
        else if (!isTemporalConstraintAboutDateTime) List(QueryColumn.ENCOUNTER)
        else {
          if (temporalConstraint.datePreference.isDefined) {
            val localDatePreference =
              temporalConstraint.datePreference.get.getOrElse(criterion.i, List[String]())
            if (localDatePreference.isEmpty)
              queryBuilderConfigs.defaultDatePreferencePerCollection(collection)
            else localDatePreference
          } else queryBuilderConfigs.defaultDatePreferencePerCollection(collection)
        }
      normalizedDatePreferenceList = normalizedDatePreferenceList ++ (if (temporalConstraint.constraintType == SAME_EPISODE_OF_CARE) {
                                                                        List(
                                                                          QueryColumn.EPISODE_OF_CARE)
                                                                      } else {
                                                                        List()
                                                                      })
      convertDatePreferenceToDateTimeSolrField(normalizedDatePreferenceList, collection)
    }

    var criterionTagsMapTmp = criterionTagsMap
    val criteria = groupResource.criteria
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
            val criterionId = criterion.i
            val collection: String = criterionTagsMapTmp(criterionId).resourceType
            var requiredSolrFieldList: List[String] =
              criterionTagsMapTmp(criterionId).requiredFieldList
            val isDateTimeAvailable: Boolean = criterionTagsMapTmp(criterionId).isDateTimeAvailable
            val isEncounterAvailable: Boolean =
              criterionTagsMapTmp(criterionId).isEncounterAvailable
            var constraintTypeList: List[String] =
              criterionTagsMapTmp(criterionId).temporalConstraintTypeList

            val isInTemporalConstraint
              : Boolean = (isEncounterAvailable && isTemporalConstraintAboutEncounter) || (isDateTimeAvailable && isTemporalConstraintAboutDateTime) || isTemporalConstraintAboutEpisodeOfCare

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
            (mergedMap(i._1).requiredFieldList ++ i._2.requiredFieldList).distinct,
            mergedMap(i._1).isResourceFilter || i._2.isResourceFilter,
            mergedMap(i._1).withOrganizations || i._2.withOrganizations,
          ))
        else mergedMap + (i._1 -> i._2)
    })
    mergedMap
  }

  private def getBasicRessourceTags(genericQuery: BasicResource,
                                    isResourceFilter: Boolean,
                                    requestOrganization: Boolean): CriterionTags = {
    var requiredSolrFieldList =
      Map(
        isResourceFilter -> QueryColumn.ID,
        requestOrganization -> QueryColumn.ORGANIZATIONS
      ).foldLeft(List[String]()) {
        case (acc, (key, value)) =>
          if (key) value :: acc else acc
      }
    val collection = genericQuery.resourceType

    def getEncounterDateRangeDatetimePreferenceList(dateTimeList: List[String]): List[String] = {
      if (genericQuery.encounterDateRange.isDefined) {
        val colsMapping = queryBuilderConfigs
          .requestKeyPerCollectionMap(collection)
        (dateTimeList ++ colsMapping
          .getOrElse(QueryColumn.ENCOUNTER_START_DATE, List[String]()) ++ colsMapping
          .getOrElse(QueryColumn.ENCOUNTER_END_DATE, List[String]())).distinct
      } else dateTimeList
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
        (solrFieldList ++ List(QueryColumn.ENCOUNTER)).distinct
      } else solrFieldList
    }

    def getResourceGroupByFieldList(solrFieldList: List[String]): List[String] = {
      if (queryBuilderConfigs
            .requestKeyPerCollectionMap(collection)
            .contains(QueryColumn.GROUP_BY)) {
        (solrFieldList ++ queryBuilderConfigs.requestKeyPerCollectionMap(collection)(
          QueryColumn.GROUP_BY)).distinct
      } else solrFieldList
    }

    def getIsDateTimeAvailable: Boolean = {
      val colsMapping = queryBuilderConfigs
        .requestKeyPerCollectionMap(collection)
      colsMapping.contains(QueryColumn.EVENT_DATE) ||
      colsMapping.contains(QueryColumn.ENCOUNTER_START_DATE) ||
      colsMapping.contains(QueryColumn.ENCOUNTER_END_DATE)
    }

    def getIsEncounterAvailable: Boolean = {
      queryBuilderConfigs
        .requestKeyPerCollectionMap(collection)
        .contains(QueryColumn.ENCOUNTER)
    }

    def getIsEpisodeOfCareAvailable: Boolean = {
      queryBuilderConfigs
        .requestKeyPerCollectionMap(collection)
        .contains(QueryColumn.EPISODE_OF_CARE)
    }

    requiredSolrFieldList = getEncounterDateRangeDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getDateRangeDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getPatientAgeDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getSameDayOccurrenceDatetimePreferenceList(requiredSolrFieldList)
    requiredSolrFieldList = getSameEncounterOccurrenceFieldList(requiredSolrFieldList)
    requiredSolrFieldList = getResourceGroupByFieldList(requiredSolrFieldList)
    requiredSolrFieldList =
      convertDatePreferenceToDateTimeSolrField(requiredSolrFieldList, collection)
    val isDateTimeAvailable: Boolean = getIsDateTimeAvailable
    val isEncounterAvailable: Boolean = getIsEncounterAvailable
    val isEpisodeOfCareAvailable: Boolean = getIsEpisodeOfCareAvailable
    new CriterionTags(
      isDateTimeAvailable,
      isEncounterAvailable,
      isEpisodeOfCareAvailable,
      false,
      List[String](),
      collection,
      requiredSolrFieldList,
      isResourceFilter,
      requestOrganization
    )
  }

  private def getGroupTags(criteria: List[BaseQuery],
                           criterionTagsMap: Map[Short, CriterionTags],
                           requestOrganization: Boolean): CriterionTags = {

    val inclusionCriteriaIdList: List[Short] =
      criteria.filter(x => x.IsInclusive).map(x => x.i)

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
      case QueryColumn.ID =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(QueryColumn.ID, List[String]())
      case QueryColumn.ORGANIZATIONS =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(QueryColumn.ORGANIZATIONS, List[String]())
      case EVENT_DATE =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(QueryColumn.EVENT_DATE, List[String]())
      case QueryColumn.ENCOUNTER =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(QueryColumn.ENCOUNTER, List[String]())
      case ENCOUNTER_START_DATE =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(QueryColumn.ENCOUNTER_START_DATE,
                                                           List[String]())
      case ENCOUNTER_END_DATE =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(QueryColumn.ENCOUNTER_END_DATE,
                                                           List[String]())
      case QueryColumn.EPISODE_OF_CARE =>
        dateTimeSolrFieldList ++= translationMap.getOrElse(QueryColumn.EPISODE_OF_CARE,
                                                           List[String]())
      case alreadyGoodString: String => dateTimeSolrFieldList ::= alreadyGoodString
    }
    dateTimeSolrFieldList.distinct
  }

}
