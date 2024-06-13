package fr.aphp.id.eds.requester.query.engine

import fr.aphp.id.eds.requester.QueryColumn.{EVENT_DATE, LOCAL_DATE}
import fr.aphp.id.eds.requester._

class QueryBuilderConfigs {

  def buildMap(patientCol: List[String], dateColListTarget: List[String]): Map[String, List[String]] = {
    Map(
      PATIENT_COL -> patientCol,
      DATE_COL -> dateColListTarget,
      ENCOUNTER_COL -> List(SolrColumn.ENCOUNTER),
      ENCOUNTER_DATES_COL -> List(SolrColumn.ENCOUNTER_START_DATE, SolrColumn.ENCOUNTER_END_DATE)
    )
  }

  def buildMap(dateColListTarget: List[String]): Map[String, List[String]] = {
    buildMap(List(SolrColumn.PATIENT), dateColListTarget)
  }

  def buildColName(id: Short, colName: String): String = {
    s"${id}_::_$colName"
  }

  val requestKeyPerCollectionMap: Map[String, Map[String, List[String]]] =
    Map(
      FhirResource.ENCOUNTER -> Map(PATIENT_COL -> List(SolrColumn.PATIENT),
        ENCOUNTER_COL -> List(SolrColumn.ENCOUNTER),
        ENCOUNTER_DATES_COL -> List(SolrColumn.Encounter.PERIOD_START, SolrColumn.Encounter.PERIOD_END)),
      FhirResource.MEDICATION_REQUEST -> buildMap(List(SolrColumn.MedicationRequest.PERIOD_START, SolrColumn.MedicationRequest.PERIOD_END)),
      FhirResource.MEDICATION_ADMINISTRATION -> buildMap(List(SolrColumn.MedicationAdministration.PERIOD_START)),
      FhirResource.OBSERVATION -> buildMap(List(SolrColumn.Observation.EFFECTIVE_DATETIME)),
      FhirResource.CONDITION -> buildMap(List(SolrColumn.Condition.RECORDED_DATE)),
      FhirResource.PATIENT -> Map(PATIENT_COL -> List(SolrColumn.PATIENT)),
      IPP_LIST -> Map(PATIENT_COL -> List(SolrColumn.PATIENT)),
      FhirResource.DOCUMENT_REFERENCE -> buildMap(List(SolrColumn.Document.DATE)),
      FhirResource.COMPOSITION -> buildMap(List(SolrColumn.Document.DATE)),
      FhirResource.GROUP -> Map(PATIENT_COL -> List(SolrColumn.Group.RESOURCE_ID)),
      FhirResource.CLAIM -> buildMap(List(SolrColumn.Claim.CREATED)),
      FhirResource.PROCEDURE -> buildMap(List(SolrColumn.Procedure.DATE)),
      FhirResource.IMAGING_STUDY -> (buildMap(List(SolrColumn.PATIENT), List(SolrColumn.ImagingStudy.STARTED,SolrColumn.ImagingStudy.SERIES_STARTED)) ++ Map(GROUP_BY_COLUMN -> List(SolrColumn.ImagingStudy.STUDY_ID))),
      FhirResource.QUESTIONNAIRE_RESPONSE -> (buildMap(List(SolrColumn.QuestionnaireResponse.AUTHORED)) ++ Map(EPISODE_OF_CARE_COL -> List(SolrColumn.EPISODE_OF_CARE))),
      FhirResource.UNKNOWN -> Map(DATE_COL -> List[String](), ENCOUNTER_COL -> List[String](), ENCOUNTER_DATES_COL -> List[String]())
    )

  val listCollectionWithEventDatetimeFields: List[String] =
    requestKeyPerCollectionMap
      .filter(el => el._2.contains(DATE_COL))
      .keys
      .toList

  val listCollectionWithEncounterFields: List[String] =
    requestKeyPerCollectionMap
      .filter(el => el._2.contains(ENCOUNTER_COL))
      .keys
      .toList

  // @todo : read default.yml instead
  val defaultDatePreferencePerCollection: Map[String, List[String]] =
    Map[String, List[String]](
      FhirResource.ENCOUNTER -> List(QueryColumn.ENCOUNTER_START_DATE, QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.CONDITION -> List(QueryColumn.ENCOUNTER_END_DATE, QueryColumn.ENCOUNTER_START_DATE, EVENT_DATE),
      FhirResource.PATIENT -> List(QueryColumn.PATIENT_BIRTHDATE),
      FhirResource.DOCUMENT_REFERENCE -> List(EVENT_DATE, QueryColumn.ENCOUNTER_START_DATE, QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.COMPOSITION -> List(EVENT_DATE, QueryColumn.ENCOUNTER_START_DATE, QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.GROUP -> List(),
      FhirResource.CLAIM -> List(QueryColumn.ENCOUNTER_END_DATE, QueryColumn.ENCOUNTER_START_DATE, EVENT_DATE),
      FhirResource.PROCEDURE -> List(EVENT_DATE, QueryColumn.ENCOUNTER_END_DATE, QueryColumn.ENCOUNTER_START_DATE),
      FhirResource.MEDICATION_ADMINISTRATION -> List(EVENT_DATE, QueryColumn.ENCOUNTER_START_DATE, QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.MEDICATION_REQUEST -> List(EVENT_DATE, QueryColumn.ENCOUNTER_START_DATE, QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.OBSERVATION -> List(EVENT_DATE, QueryColumn.ENCOUNTER_START_DATE, QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.IMAGING_STUDY -> List(EVENT_DATE, QueryColumn.ENCOUNTER_START_DATE, QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.QUESTIONNAIRE_RESPONSE -> List(EVENT_DATE, QueryColumn.ENCOUNTER_START_DATE, QueryColumn.ENCOUNTER_END_DATE),
      IPP_LIST -> List[String](),
      FhirResource.UNKNOWN -> List(QueryColumn.ENCOUNTER_START_DATE, QueryColumn.ENCOUNTER_END_DATE, EVENT_DATE)
    )

  def getSubjectColumn(id: Short, isPatient: Boolean = true): String = buildColName(id, if (isPatient) QueryColumn.PATIENT else SolrColumn.ID)

  def getEncounterColumn(id: Short): String = buildColName(id, QueryColumn.ENCOUNTER)

  def getEpisodeOfCareColumn(id: Short): String = buildColName(id, QueryColumn.EPISODE_OF_CARE)

  def getDateColumn(id: Short): String = buildColName(id, LOCAL_DATE)

  def getEventDateColumn(id: Short): String = buildColName(id, EVENT_DATE)

  def getOrganizationsColumn(id: Short): String = buildColName(id, QueryColumn.ORGANIZATIONS)

  def getEncounterStartDateColumn(id: Short): String = buildColName(id, QueryColumn.ENCOUNTER_START_DATE)

  def getEncounterEndDateColumn(id: Short): String = buildColName(id, QueryColumn.ENCOUNTER_END_DATE)

  def getPatientBirthColumn(id: Short): String = buildColName(id, QueryColumn.PATIENT_BIRTHDATE)

}
