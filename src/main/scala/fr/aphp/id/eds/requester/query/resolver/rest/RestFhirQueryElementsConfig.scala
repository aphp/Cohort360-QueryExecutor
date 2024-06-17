package fr.aphp.id.eds.requester.query.resolver.rest

import fr.aphp.id.eds.requester.{FhirResource, QueryColumn}
import fr.aphp.id.eds.requester.query.resolver.ResourceConfig
import org.hl7.fhir.instance.model.api.IBase

case class ResourceMapping(fhirPath: String,
                                       colName: String,
                                       fhirType: Class[_ <: IBase],
                                       nullable: Boolean = true)

class RestFhirQueryElementsConfig extends ResourceConfig {

  override def requestKeyPerCollectionMap: Map[String, Map[String, List[String]]] = Map(
    FhirResource.PATIENT -> Map(
      QueryColumn.PATIENT -> List("id"),
      QueryColumn.EVENT_DATE -> List("birthDate"),
    ),
    FhirResource.ENCOUNTER -> Map(
      QueryColumn.PATIENT -> List("subject"),
      QueryColumn.EVENT_DATE -> List("period"),
      QueryColumn.ENCOUNTER_START_DATE -> List("period.start"),
      QueryColumn.ENCOUNTER_END_DATE -> List("period.end"),
    ),
    FhirResource.OBSERVATION -> Map(
      QueryColumn.PATIENT -> List("subject"),
      QueryColumn.ENCOUNTER -> List("encounter"),
      QueryColumn.EVENT_DATE -> List("effectiveDateTime"),
      QueryColumn.ENCOUNTER_START_DATE -> List("encounter.period.start"),
      QueryColumn.ENCOUNTER_END_DATE -> List("encounter.period.end"),
    ),
    FhirResource.CONDITION -> Map(
      QueryColumn.PATIENT -> List("subject"),
      QueryColumn.ENCOUNTER -> List("encounter"),
      QueryColumn.EVENT_DATE -> List("recordedDate"),
      QueryColumn.ENCOUNTER_START_DATE -> List("encounter.period.start"),
      QueryColumn.ENCOUNTER_END_DATE -> List("encounter.period.end"),
    ),
    FhirResource.MEDICATION_REQUEST -> Map(
      QueryColumn.PATIENT -> List("subject"),
      QueryColumn.ENCOUNTER -> List("encounter"),
      QueryColumn.EVENT_DATE -> List("authoredOn"),
      QueryColumn.ENCOUNTER_START_DATE -> List("encounter.period.start"),
      QueryColumn.ENCOUNTER_END_DATE -> List("encounter.period.end"),
    ),
    FhirResource.MEDICATION_ADMINISTRATION -> Map(
      QueryColumn.PATIENT -> List("subject"),
      QueryColumn.ENCOUNTER -> List("encounter"),
      QueryColumn.EVENT_DATE -> List("effectiveDateTime"),
      QueryColumn.ENCOUNTER_START_DATE -> List("encounter.period.start"),
      QueryColumn.ENCOUNTER_END_DATE -> List("encounter.period.end"),
    ),
    FhirResource.DOCUMENT_REFERENCE -> Map(
      QueryColumn.PATIENT -> List("subject"),
      QueryColumn.ENCOUNTER -> List("encounter"),
      QueryColumn.EVENT_DATE -> List("date"),
      QueryColumn.ENCOUNTER_START_DATE -> List("encounter.period.start"),
      QueryColumn.ENCOUNTER_END_DATE -> List("encounter.period.end"),
    ),
    FhirResource.CLAIM -> Map(
      QueryColumn.PATIENT -> List("patient.reference"),
      QueryColumn.ENCOUNTER -> List("encounter"),
      QueryColumn.EVENT_DATE -> List("created"),
      QueryColumn.ENCOUNTER_START_DATE -> List("encounter.period.start"),
      QueryColumn.ENCOUNTER_END_DATE -> List("encounter.period.end"),
    ),
    FhirResource.PROCEDURE -> Map(
      QueryColumn.PATIENT -> List("subject"),
      QueryColumn.ENCOUNTER -> List("encounter"),
      QueryColumn.EVENT_DATE -> List("date"),
      QueryColumn.ENCOUNTER_START_DATE -> List("encounter.period.start"),
      QueryColumn.ENCOUNTER_END_DATE -> List("encounter.period.end"),
    ),
    FhirResource.IMAGING_STUDY -> Map(
      QueryColumn.PATIENT -> List("subject"),
      QueryColumn.ENCOUNTER -> List("encounter"),
      QueryColumn.EVENT_DATE -> List("started"),
      QueryColumn.ENCOUNTER_START_DATE -> List("encounter.period.start"),
      QueryColumn.ENCOUNTER_END_DATE -> List("encounter.period.end"),
    ),
    FhirResource.QUESTIONNAIRE_RESPONSE -> Map(
      QueryColumn.PATIENT -> List("subject"),
      QueryColumn.ENCOUNTER -> List("encounter"),
      QueryColumn.EVENT_DATE -> List("authored"),
      QueryColumn.EPISODE_OF_CARE -> List("encounter.episodeOfCare"),
      QueryColumn.ENCOUNTER_START_DATE -> List("encounter.period.start"),
      QueryColumn.ENCOUNTER_END_DATE -> List("encounter.period.end"),
    )
  )

  override def reverseColumnMapping(collection: String, column_name: String): String = {
    requestKeyPerCollectionMap(collection)
      .find(_._2.contains(column_name))
      .map(_._1)
      .getOrElse(column_name.replace(".", "_"))
  }

  val fhirPathMappings: Map[String, List[ResourceMapping]] = Map(
    FhirResource.PATIENT -> List(
      ResourceMapping("Patient.id", QueryColumn.PATIENT, classOf[IBase]),
      ResourceMapping("Patient.birthDate", QueryColumn.PATIENT_BIRTHDATE, classOf[IBase]),
    ),
    FhirResource.ENCOUNTER -> List(
      ResourceMapping("Encounter.subject", QueryColumn.PATIENT, classOf[IBase]),
      ResourceMapping("Encounter.id", QueryColumn.ENCOUNTER, classOf[IBase]),
      ResourceMapping("Encounter.period.start", QueryColumn.ENCOUNTER_START_DATE, classOf[IBase]),
      ResourceMapping("Encounter.period.end", QueryColumn.ENCOUNTER_END_DATE, classOf[IBase]),
    ),
    FhirResource.OBSERVATION -> List(
      ResourceMapping("Observation.id", QueryColumn.ID, classOf[IBase]),
      ResourceMapping("Observation.subject", QueryColumn.PATIENT, classOf[IBase]),
      ResourceMapping("Observation.effectiveDateTime", QueryColumn.EVENT_DATE, classOf[IBase]),
    ),
    FhirResource.CONDITION -> List(
      ResourceMapping("Condition.id", "id", classOf[IBase]),
      ResourceMapping("Condition.recordedDate", QueryColumn.EVENT_DATE, classOf[IBase]),
    ),
    FhirResource.MEDICATION_REQUEST -> List(
      ResourceMapping("MedicationRequest.id", "id ", classOf[IBase]),
      ResourceMapping("MedicationRequest.period.start", QueryColumn.EVENT_DATE, classOf[IBase]),
      ResourceMapping("MedicationRequest.period.end", QueryColumn.EVENT_DATE, classOf[IBase]),
    ),
    FhirResource.MEDICATION_ADMINISTRATION -> List(
      ResourceMapping("MedicationAdministration.id", "id", classOf[IBase]),
      ResourceMapping("MedicationAdministration.period.start",
                      QueryColumn.EVENT_DATE,
                      classOf[IBase]),
    ),
    FhirResource.DOCUMENT_REFERENCE -> List(
      ResourceMapping("DocumentReference.id", "id", classOf[IBase]),
      ResourceMapping("DocumentReference.date", QueryColumn.EVENT_DATE, classOf[IBase]),
    ),
    FhirResource.CLAIM -> List(
      ResourceMapping("Claim.id", "id", classOf[IBase]),
      ResourceMapping("Claim.created", QueryColumn.EVENT_DATE, classOf[IBase]),
    ),
    FhirResource.PROCEDURE -> List(
      ResourceMapping("Procedure.id", "id", classOf[IBase]),
      ResourceMapping("Procedure.date", QueryColumn.EVENT_DATE, classOf[IBase]),
    ),
    FhirResource.IMAGING_STUDY -> List(
      ResourceMapping("ImagingStudy.id", "id", classOf[IBase]),
      ResourceMapping("ImagingStudy.patient", QueryColumn.PATIENT, classOf[IBase]),
      ResourceMapping("ImagingStudy.started", QueryColumn.EVENT_DATE, classOf[IBase]),
      ResourceMapping("ImagingStudy.id", QueryColumn.GROUP_BY, classOf[IBase]),
    ),
    FhirResource.QUESTIONNAIRE_RESPONSE -> List(
      ResourceMapping("QuestionnaireResponse.id", "id", classOf[IBase]),
      ResourceMapping("QuestionnaireResponse.authored", QueryColumn.EVENT_DATE, classOf[IBase]),
      ResourceMapping("QuestionnaireResponse.episodeOfCare",
                      QueryColumn.EPISODE_OF_CARE,
                      classOf[IBase]),
    )
  )
}
