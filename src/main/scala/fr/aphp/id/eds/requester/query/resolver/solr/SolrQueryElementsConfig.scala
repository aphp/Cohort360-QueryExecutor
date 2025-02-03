package fr.aphp.id.eds.requester.query.resolver.solr

import fr.aphp.id.eds.requester.QueryColumn.EVENT_DATE
import fr.aphp.id.eds.requester.query.resolver.ResourceConfig
import fr.aphp.id.eds.requester.{FhirResource, QueryColumn}

class SolrQueryElementsConfig extends ResourceConfig {

  def buildMap(patientCol: List[String],
               dateColListTarget: List[String], codeCol: Option[List[String]] = None): Map[String, List[String]] = {
    Map(
      QueryColumn.PATIENT -> patientCol,
      QueryColumn.EVENT_DATE -> dateColListTarget,
      QueryColumn.ENCOUNTER -> List(SolrColumn.ENCOUNTER),
      QueryColumn.ENCOUNTER_START_DATE -> List(SolrColumn.ENCOUNTER_START_DATE),
      QueryColumn.ENCOUNTER_END_DATE -> List(SolrColumn.ENCOUNTER_END_DATE),
      QueryColumn.ID -> List(SolrColumn.ID),
      QueryColumn.ORGANIZATIONS -> List(SolrColumn.ORGANIZATIONS),
    ) ++ codeCol.map(QueryColumn.CODE -> _).toMap
  }

  def buildDefaultMap(dateColListTarget: List[String], codeCol: Option[List[String]] = None): Map[String, List[String]] = {
    buildMap(List(SolrColumn.PATIENT), dateColListTarget, codeCol)
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
    FhirResource.MEDICATION_REQUEST -> buildDefaultMap(
      List(SolrColumn.MedicationRequest.PERIOD_START, SolrColumn.MedicationRequest.PERIOD_END),
      codeCol = Some(List(SolrColumn.MedicationRequest.CODE_ATC, SolrColumn.MedicationRequest.CODE_UCD))
    ),
    FhirResource.MEDICATION_ADMINISTRATION -> buildDefaultMap(
      List(SolrColumn.MedicationAdministration.PERIOD_START),
      codeCol = Some(List(SolrColumn.MedicationAdministration.CODE_ATC, SolrColumn.MedicationAdministration.CODE_UCD))
    ),
    FhirResource.OBSERVATION -> buildDefaultMap(List(SolrColumn.Observation.EFFECTIVE_DATETIME),
      codeCol = Some(List(SolrColumn.Observation.CODE))
    ),
    FhirResource.CONDITION -> buildDefaultMap(List(SolrColumn.Condition.RECORDED_DATE),
      codeCol = Some(List(SolrColumn.Condition.CODE))
    ),
    FhirResource.PATIENT -> Map(QueryColumn.PATIENT -> List(SolrColumn.PATIENT),
                                QueryColumn.ID -> List(SolrColumn.ID),
                                QueryColumn.ORGANIZATIONS -> List(SolrColumn.ORGANIZATIONS)),
    FhirResource.DOCUMENT_REFERENCE -> buildDefaultMap(List(SolrColumn.Document.DATE)),
    FhirResource.COMPOSITION -> buildDefaultMap(List(SolrColumn.Document.DATE)),
    FhirResource.GROUP -> Map(QueryColumn.PATIENT -> List(SolrColumn.Group.RESOURCE_ID),
                              QueryColumn.ID -> List(SolrColumn.ID)),
    FhirResource.CLAIM -> buildDefaultMap(List(SolrColumn.Claim.CREATED),
      codeCol = Some(List(SolrColumn.Claim.CODE))
    ),
    FhirResource.PROCEDURE -> buildDefaultMap(List(SolrColumn.Procedure.DATE),
      codeCol = Some(List(SolrColumn.Procedure.CODE))
    ),
    FhirResource.IMAGING_STUDY -> (buildMap(List(SolrColumn.PATIENT),
                                            List(SolrColumn.ImagingStudy.STARTED,
                                                 SolrColumn.ImagingStudy.SERIES_STARTED)) ++ Map(
      QueryColumn.GROUP_BY -> List(SolrColumn.ImagingStudy.STUDY_ID))),
    FhirResource.QUESTIONNAIRE_RESPONSE -> (buildDefaultMap(
      List(SolrColumn.QuestionnaireResponse.AUTHORED)) ++ Map(
      QueryColumn.EPISODE_OF_CARE -> List(SolrColumn.EPISODE_OF_CARE))),
    FhirResource.UNKNOWN -> Map()
  )

  def reverseColumnMapping(collection: String, columnName: String): String = {
    val dateField =
      requestKeyPerCollectionMap(collection)
        .getOrElse(QueryColumn.EVENT_DATE, List[String](""))
        .head
    val patientField =
      requestKeyPerCollectionMap(collection).getOrElse(QueryColumn.PATIENT, List[String]("")).head
    val encounterField =
      requestKeyPerCollectionMap(collection).getOrElse(QueryColumn.ENCOUNTER, List("")).head
    val groupByField =
      requestKeyPerCollectionMap(collection).getOrElse(QueryColumn.GROUP_BY, List("")).head
    collection match {
      case FhirResource.ENCOUNTER =>
        columnName match {
          case SolrColumn.Encounter.PERIOD_START => QueryColumn.ENCOUNTER_START_DATE
          case SolrColumn.Encounter.PERIOD_END   => QueryColumn.ENCOUNTER_END_DATE
          case SolrColumn.PATIENT_BIRTHDATE      => QueryColumn.PATIENT_BIRTHDATE
          case SolrColumn.ORGANIZATIONS          => QueryColumn.ORGANIZATIONS
          case `dateField`                       => EVENT_DATE
          case `patientField`                    => QueryColumn.PATIENT
          case `encounterField`                  => QueryColumn.ENCOUNTER
          case _                                 => columnName.replace(".", "_")
        }
      case FhirResource.PATIENT =>
        columnName match {
          case SolrColumn.Patient.BIRTHDATE => QueryColumn.PATIENT_BIRTHDATE
          case SolrColumn.ORGANIZATIONS     => QueryColumn.ORGANIZATIONS
          case `dateField`                  => QueryColumn.EVENT_DATE
          case `patientField`               => QueryColumn.PATIENT
          case _                            => columnName.replace(".", "_")
        }
      case _ =>
        columnName match {
          case `dateField`                     => QueryColumn.EVENT_DATE
          case `patientField`                  => QueryColumn.PATIENT
          case SolrColumn.PATIENT_BIRTHDATE    => QueryColumn.PATIENT_BIRTHDATE
          case SolrColumn.ENCOUNTER_START_DATE => QueryColumn.ENCOUNTER_START_DATE
          case SolrColumn.ENCOUNTER_END_DATE   => QueryColumn.ENCOUNTER_END_DATE
          case SolrColumn.ORGANIZATIONS        => QueryColumn.ORGANIZATIONS
          case SolrColumn.EPISODE_OF_CARE      => QueryColumn.EPISODE_OF_CARE
          case `encounterField`                => QueryColumn.ENCOUNTER
          case `groupByField`                  => QueryColumn.GROUP_BY
          case _                               => columnName.replace(".", "_")
        }
    }
  }
}
