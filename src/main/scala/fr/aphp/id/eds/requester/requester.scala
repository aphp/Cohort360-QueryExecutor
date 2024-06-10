package fr.aphp.id.eds

package object requester {
  object FhirResource {
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
    final val DEFAULT = "default"

  }

  object QueryColumn {
    final val PATIENT = "patient"
    final val ENCOUNTER = "encounter"
    final val EPISODE_OF_CARE = "episodeOfCare"
    final val ENCOUNTER_START_DATE = "encounter_start_date"
    final val ENCOUNTER_END_DATE = "encounter_end_date"
    final val PATIENT_BIRTHDATE = "patient_birthdate"
    final val EVENT_DATE = "event_date"
    final val LOCAL_DATE = "localDate"
    final val AGE = "age"
    final val ORGANIZATIONS = "organizations"
  }

  object ResultColumn {
    final val SUBJECT = "subject_id"
    final val ORGANIZATIONS = "organization_ids"
    final val ORGANIZATION = "organization_id"
  }

  object SolrColumn {
    final val ID = "id"
    final val PATIENT = "_subject"
    final val ENCOUNTER = "_visit"
    final val EPISODE_OF_CARE = "episodeOfCare"
    final val PATIENT_PREFIX = "_ref.patient."
    final val PATIENT_BIRTHDATE = PATIENT_PREFIX + Patient.BIRTHDATE
    final val ENCOUNTER_PREFIX = "_ref.encounter."
    final val ENCOUNTER_START_DATE = ENCOUNTER_PREFIX + Encounter.PERIOD_START
    final val ENCOUNTER_END_DATE = ENCOUNTER_PREFIX + Encounter.PERIOD_END
    final val ORGANIZATIONS = "_list.organization"

    object Patient {
      final val BIRTHDATE = "birthdate"
    }

    object Encounter {
      final val PERIOD_START = "period.start"
      final val PERIOD_END = "period.end"
    }

    object MedicationAdministration {
      final val PERIOD_START = "effectivePeriod.start"
    }

    object MedicationRequest {
      final val PERIOD_START = "dispenseRequest.validityPeriod.start"
      final val PERIOD_END = "dispenseRequest.validityPeriod.end"
    }

    object Observation {
      final val EFFECTIVE_DATETIME = "effectiveDateTime"
    }

    object Claim {
      final val CREATED = "created"
    }

    object Condition {
      final val RECORDED_DATE = "recordedDate"
    }

    object Procedure {
      final val DATE = "performedDateTime"
    }

    object Document {
      final val DATE = "date"
    }

    object Group {
      final val RESOURCE_ID = "resourceId"
    }

    object ImagingStudy {
      final val STARTED = "started"
      final val SERIES_STARTED = "series.started"
      final val STUDY_ID = "studyId"
    }

    object QuestionnaireResponse {
      final val AUTHORED = "authored"
    }
  }

  final val IPP_LIST = "IPPList"
  final val PATIENT_COL = "patient_col"
  final val DATE_COL = "date_col"
  final val ENCOUNTER_ID = "encounter_id"
  final val EPISODE_OF_CARE_COL = "episode_of_care_col"
  final val ENCOUNTER_COL = "encounter_col"
  final val ENCOUNTER_DATES_COL = "encounter_dates_col"
  final val GROUP_BY_COLUMN = "group_by_column"

}
