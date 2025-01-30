package fr.aphp.id.eds.requester.query.resolver

package object solr {
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
      final val CODE_ATC = "_sort.atc"
      final val CODE_UCD = "_sort.ucd"
    }

    object MedicationRequest {
      final val PERIOD_START = "dispenseRequest.validityPeriod.start"
      final val PERIOD_END = "dispenseRequest.validityPeriod.end"
      final val CODE_ATC = "_sort.atc"
      final val CODE_UCD = "_sort.ucd"
    }

    object Observation {
      final val EFFECTIVE_DATETIME = "effectiveDateTime"
      final val CODE = "code.coding.display.anabio"
    }

    object Claim {
      final val CREATED = "created"
      final val CODE = "diagnosis.diagnosisCodeableConcept.coding.display"
    }

    object Condition {
      final val RECORDED_DATE = "recordedDate"
      final val CODE = "code.coding.display"
    }

    object Procedure {
      final val DATE = "performedDateTime"
      final val CODE = "code.coding.display"
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
}
