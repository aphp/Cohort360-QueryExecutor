package fr.aphp.id.eds

package object requester {
  object FhirResource {
    final val UNKNOWN = "unknown"
    final val PATIENT = "Patient"
    final val ENCOUNTER = "Encounter"
    final val OBSERVATION = "Observation"
    final val CONDITION = "Condition"
    final val PROCEDURE = "Procedure"
    final val DOCUMENT_REFERENCE = "DocumentReference"
    final val CLAIM = "Claim"
    final val COMPOSITION = "Composition"
    final val GROUP = "Group"
    final val MEDICATION_REQUEST = "MedicationRequest"
    final val MEDICATION_ADMINISTRATION = "MedicationAdministration"
    final val IMAGING_STUDY = "ImagingStudy"
    final val QUESTIONNAIRE_RESPONSE = "QuestionnaireResponse"
  }

  object SolrCollection {
    final val PATIENT_APHP = "patientAphp"
    final val ENCOUNTER_APHP = "encounterAphp"
    final val OBSERVATION_APHP = "observationAphp"
    final val CONDITION_APHP = "conditionAphp"
    final val PROCEDURE_APHP = "procedureAphp"
    final val DOCUMENTREFERENCE_APHP = "documentReferenceAphp"
    final val CLAIM_APHP = "claimAphp"
    final val COMPOSITION_APHP = "compositionAphp"
    final val GROUP_APHP = "groupAphp"
    final val MEDICATIONREQUEST_APHP = "medicationRequestAphp"
    final val MEDICATIONADMINISTRATION_APHP = "medicationAdministrationAphp"
    final val IMAGINGSTUDY_APHP = "imagingStudyAphp"
    final val QUESTIONNAIRE_RESPONSE_APHP = "questionnaireResponseAphp"

  }

  object QueryColumn {
    final val ID = "id"
    final val PATIENT = "patient"
    final val ENCOUNTER = "encounter"
    final val EPISODE_OF_CARE = "episodeOfCare"
    final val ENCOUNTER_START_DATE = "encounter_start_date"
    final val ENCOUNTER_END_DATE = "encounter_end_date"
    final val PATIENT_BIRTHDATE = "patient_birthdate"
    final val CODE = "code"
    final val EVENT_DATE = "event_date"
    final val LOCAL_DATE = "localDate"
    final val AGE = "age"
    final val ORGANIZATIONS = "organizations"
    final val GROUP_BY = "group_by"
  }

  object ResultColumn {
    final val SUBJECT = "subject_id"
    final val ORGANIZATIONS = "organization_ids"
    final val ORGANIZATION = "organization_id"
  }

}
