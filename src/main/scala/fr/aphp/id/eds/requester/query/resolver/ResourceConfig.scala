package fr.aphp.id.eds.requester.query.resolver

import fr.aphp.id.eds.requester.QueryColumn.{EVENT_DATE, LOCAL_DATE}
import fr.aphp.id.eds.requester._

abstract class ResourceConfig {

  def requestKeyPerCollectionMap: Map[String, Map[String, List[String]]]

  def reverseColumnMapping(collection: String, column_name: String): String

  def buildColName(id: Short, colName: String): String = {
    s"${id}_::_$colName"
  }

  def listCollectionWithEventDatetimeFields: List[String] =
    requestKeyPerCollectionMap
      .filter(el => el._2.contains(QueryColumn.EVENT_DATE))
      .keys
      .toList

  def listCollectionWithEncounterFields: List[String] =
    requestKeyPerCollectionMap
      .filter(el => el._2.contains(QueryColumn.ENCOUNTER) && el._2(QueryColumn.ENCOUNTER).nonEmpty)
      .keys
      .toList

  // @todo : read default.yml instead
  val defaultDatePreferencePerCollection: Map[String, List[String]] =
    Map[String, List[String]](
      FhirResource.ENCOUNTER -> List(QueryColumn.ENCOUNTER_START_DATE,
                                     QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.CONDITION -> List(QueryColumn.ENCOUNTER_END_DATE,
                                     QueryColumn.ENCOUNTER_START_DATE,
                                     EVENT_DATE),
      FhirResource.PATIENT -> List(QueryColumn.PATIENT_BIRTHDATE),
      FhirResource.DOCUMENT_REFERENCE -> List(EVENT_DATE,
                                              QueryColumn.ENCOUNTER_START_DATE,
                                              QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.COMPOSITION -> List(EVENT_DATE,
                                       QueryColumn.ENCOUNTER_START_DATE,
                                       QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.GROUP -> List(),
      FhirResource.CLAIM -> List(QueryColumn.ENCOUNTER_END_DATE,
                                 QueryColumn.ENCOUNTER_START_DATE,
                                 EVENT_DATE),
      FhirResource.PROCEDURE -> List(EVENT_DATE,
                                     QueryColumn.ENCOUNTER_END_DATE,
                                     QueryColumn.ENCOUNTER_START_DATE),
      FhirResource.MEDICATION_ADMINISTRATION -> List(EVENT_DATE,
                                                     QueryColumn.ENCOUNTER_START_DATE,
                                                     QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.MEDICATION_REQUEST -> List(EVENT_DATE,
                                              QueryColumn.ENCOUNTER_START_DATE,
                                              QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.OBSERVATION -> List(EVENT_DATE,
                                       QueryColumn.ENCOUNTER_START_DATE,
                                       QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.IMAGING_STUDY -> List(EVENT_DATE,
                                         QueryColumn.ENCOUNTER_START_DATE,
                                         QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.QUESTIONNAIRE_RESPONSE -> List(EVENT_DATE,
                                                  QueryColumn.ENCOUNTER_START_DATE,
                                                  QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.UNKNOWN -> List(QueryColumn.ENCOUNTER_START_DATE,
                                   QueryColumn.ENCOUNTER_END_DATE,
                                   EVENT_DATE)
    )

  def getSubjectColumn(id: Short, isPatient: Boolean = true): String =
    buildColName(id, if (isPatient) QueryColumn.PATIENT else QueryColumn.ID)

  def getEncounterColumn(id: Short): String = buildColName(id, QueryColumn.ENCOUNTER)

  def getEpisodeOfCareColumn(id: Short): String = buildColName(id, QueryColumn.EPISODE_OF_CARE)

  def getDateColumn(id: Short): String = buildColName(id, LOCAL_DATE)

  def getEventDateColumn(id: Short): String = buildColName(id, EVENT_DATE)

  def getOrganizationsColumn(id: Short): String = buildColName(id, QueryColumn.ORGANIZATIONS)

  def getEncounterStartDateColumn(id: Short): String =
    buildColName(id, QueryColumn.ENCOUNTER_START_DATE)

  def getEncounterEndDateColumn(id: Short): String =
    buildColName(id, QueryColumn.ENCOUNTER_END_DATE)

  def getPatientBirthColumn(id: Short): String = buildColName(id, QueryColumn.PATIENT_BIRTHDATE)

}
