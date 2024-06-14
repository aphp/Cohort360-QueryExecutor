package fr.aphp.id.eds.requester.query.resolver.solr

import fr.aphp.id.eds.requester.QueryColumn.EVENT_DATE
import fr.aphp.id.eds.requester.{FhirResource, QueryColumn}
import fr.aphp.id.eds.requester.query.resolver.QueryElementsConfig

class SolrQueryElementsConfig extends QueryElementsConfig {

  def buildMap(patientCol: List[String],
               dateColListTarget: List[String]): Map[String, List[String]] = {
    Map(
      QueryColumn.PATIENT -> patientCol,
      QueryColumn.EVENT_DATE -> dateColListTarget,
      QueryColumn.ENCOUNTER -> List(SolrColumn.ENCOUNTER),
      QueryColumn.ENCOUNTER_START_DATE -> List(SolrColumn.ENCOUNTER_START_DATE),
      QueryColumn.ENCOUNTER_END_DATE -> List(SolrColumn.ENCOUNTER_END_DATE),
      QueryColumn.ID -> List(SolrColumn.ID),
      QueryColumn.ORGANIZATIONS -> List(SolrColumn.ORGANIZATIONS)
    )
  }

  def buildMap(dateColListTarget: List[String]): Map[String, List[String]] = {
    buildMap(List(SolrColumn.PATIENT), dateColListTarget)
  }

  override def requestKeyPerCollectionMap: Map[String, Map[String, List[String]]] = Map(
    FhirResource.ENCOUNTER -> Map(
      QueryColumn.PATIENT -> List(SolrColumn.PATIENT),
      QueryColumn.ENCOUNTER -> List(SolrColumn.ENCOUNTER),
      QueryColumn.ENCOUNTER_START_DATE -> List(SolrColumn.Encounter.PERIOD_START),
      QueryColumn.ENCOUNTER_END_DATE -> List(SolrColumn.Encounter.PERIOD_END),
      QueryColumn.ID -> List(SolrColumn.ID),
      QueryColumn.ORGANIZATIONS -> List(SolrColumn.ORGANIZATIONS)
    ),
    FhirResource.MEDICATION_REQUEST -> buildMap(
      List(SolrColumn.MedicationRequest.PERIOD_START, SolrColumn.MedicationRequest.PERIOD_END)),
    FhirResource.MEDICATION_ADMINISTRATION -> buildMap(
      List(SolrColumn.MedicationAdministration.PERIOD_START)),
    FhirResource.OBSERVATION -> buildMap(List(SolrColumn.Observation.EFFECTIVE_DATETIME)),
    FhirResource.CONDITION -> buildMap(List(SolrColumn.Condition.RECORDED_DATE)),
    FhirResource.PATIENT -> Map(QueryColumn.PATIENT -> List(SolrColumn.PATIENT), QueryColumn.ID -> List(SolrColumn.ID), QueryColumn.ORGANIZATIONS -> List(SolrColumn.ORGANIZATIONS)),
    FhirResource.DOCUMENT_REFERENCE -> buildMap(List(SolrColumn.Document.DATE)),
    FhirResource.COMPOSITION -> buildMap(List(SolrColumn.Document.DATE)),
    FhirResource.GROUP -> Map(QueryColumn.PATIENT -> List(SolrColumn.Group.RESOURCE_ID), QueryColumn.ID -> List(SolrColumn.ID)),
    FhirResource.CLAIM -> buildMap(List(SolrColumn.Claim.CREATED)),
    FhirResource.PROCEDURE -> buildMap(List(SolrColumn.Procedure.DATE)),
    FhirResource.IMAGING_STUDY -> (buildMap(List(SolrColumn.PATIENT),
      List(SolrColumn.ImagingStudy.STARTED,
        SolrColumn.ImagingStudy.SERIES_STARTED)) ++ Map(
      QueryColumn.GROUP_BY -> List(SolrColumn.ImagingStudy.STUDY_ID))),
    FhirResource.QUESTIONNAIRE_RESPONSE -> (buildMap(
      List(SolrColumn.QuestionnaireResponse.AUTHORED)) ++ Map(
      QueryColumn.EPISODE_OF_CARE -> List(SolrColumn.EPISODE_OF_CARE))),
    FhirResource.UNKNOWN -> Map()
  )

  override def reverseColumnMapping(collection: String, column_name: String): String = {
    val dateField =
      requestKeyPerCollectionMap(collection).getOrElse(QueryColumn.EVENT_DATE, List[String]("")).head
    val patientField =
      requestKeyPerCollectionMap(collection).getOrElse(QueryColumn.PATIENT, List[String]("")).head
    val encounterField =
      requestKeyPerCollectionMap(collection).getOrElse(QueryColumn.ENCOUNTER, List("")).head
    collection match {
      case FhirResource.ENCOUNTER =>
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
      case FhirResource.PATIENT =>
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
          case SolrColumn.EPISODE_OF_CARE      => QueryColumn.EPISODE_OF_CARE
          case `encounterField`                => QueryColumn.ENCOUNTER
          case _                               => column_name.replace(".", "_")
        }
    }
  }
}
