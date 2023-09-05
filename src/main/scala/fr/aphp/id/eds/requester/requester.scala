package fr.aphp.id.eds

package object requester {
  object SolrCollection {
    final val PATIENT_APHP = "patientAphp"
    final val ENCOUNTER_APHP = "encounterAphp"
    final val OBSERVATION_APHP = "observationAphp"
    final val CONDITION_APHP = "conditionAphp"
    final val PROCEDURE_APHP = "procedureAphp"
    final val DOCUMENTREFERENCE_APHP = "documentReferenceAphp"
    final val CLAIM_APHP = "claimAphp"
    final val COMPOSITION_APHP = "compositionAphp"
    final val VALUESET_APHP = "valueSetAphp"
    final val ORGANIZATION_APHP = "organizationAphp"
    final val GROUP_APHP = "groupAphp"
    final val MEDICATIONREQUEST_APHP = "medicationRequestAphp"
    final val MEDICATIONADMINISTRATION_APHP = "medicationAdministrationAphp"
    final val IMAGINGSERIES_APHP = "imagingSeriesAphp"
    final val IMAGINGSTUDY_APHP = "imagingStudyAphp"
    final val CONCEPT_MAP_APHP = "conceptMapAphp"
    final val DEFAULT = "default"
    final val LIST_HIVE_FHIR_TABLE = List(PATIENT_APHP, ENCOUNTER_APHP, OBSERVATION_APHP,
      CONDITION_APHP, PROCEDURE_APHP, DOCUMENTREFERENCE_APHP, CLAIM_APHP, COMPOSITION_APHP, VALUESET_APHP,
      ORGANIZATION_APHP, GROUP_APHP, MEDICATIONREQUEST_APHP, MEDICATIONADMINISTRATION_APHP, CONCEPT_MAP_APHP)

  }

  object SolrColumn {
    final val ID = "id"
    final val PATIENT = "patient"
    final val SUBJECT = "subject"
    final val AGE = "age"
    final val ENCOUNTER = "encounter"
    final val ENCOUNTER_PREFIX = "Encounter."
    final val ENCOUNTER_START_DATE = ENCOUNTER_PREFIX + Encounter.PERIOD_START
    final val ENCOUNTER_END_DATE = ENCOUNTER_PREFIX + Encounter.PERIOD_END

    object Patient {
      final val PREFIX = "patient_"
      final val BIRTHDATE = "birthdate"
      final val PATIENT_BIRTHDATE = PREFIX + BIRTHDATE

      final val IDENTIFIER_VALUE = "identifier.value"

    }

    object Encounter {
      final val PREFIX = "encounter_"
      final val PERIOD_START = "period.start"
      final val PERIOD_END = "period.end"
      final val ENCOUNTER_START_DATE = PREFIX + "period_start"
      final val ENCOUNTER_END_DATE = PREFIX + "period_end"
    }

    object Medication {
      final val PERIOD_START = "Period-start"
      final val PERIOD_END = "Period-end"
    }

    object Observation {
      final val EFFECTIVE_DATETIME = "effectiveDatetime"
    }

    object Claim {
      final val CREATED = "created"
    }

    object Condition {
      final val RECORDED_DATE = "recorded-date"
    }

    object Procedure {
      final val DATE = "date"
    }

    object Document {
      final val DATE = "date"
    }

    object Group {
      final val RESOURCE_ID = "resourceId"
    }
  }

  final val IPP_LIST = "IPPList"
  final val PATIENT_COL = "patient_col"
  final val DATE_COL = "date_col"
  final val ENCOUNTER_ID = "encounter_id"
  final val ENCOUNTER_COL = "encounter_col"
  final val ENCOUNTER_DATES_COL = "encounter_dates_col"
  final val EVENT_DATE = "event_date"
  final val LOCAL_DATE = "localDate"


}
