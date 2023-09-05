package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.{DATE_COL, ENCOUNTER_COL, ENCOUNTER_DATES_COL, EVENT_DATE, IPP_LIST, LOCAL_DATE, PATIENT_COL, SolrCollection, SolrColumn}

class QueryBuilderConfigs {
  def buildMap(dateColListTarget: List[String]): Map[String, List[String]] = {
    Map(
      PATIENT_COL -> List(SolrColumn.PATIENT),
      DATE_COL -> dateColListTarget,
      ENCOUNTER_COL -> List(SolrColumn.ENCOUNTER),
      ENCOUNTER_DATES_COL -> List(SolrColumn.ENCOUNTER_START_DATE, SolrColumn.ENCOUNTER_END_DATE)
    )
  }

  def buildColName(id: Short, colName: String): String = {
    s"${id}_::_$colName"
  }

  val requestKeyPerCollectionMap: Map[String, Map[String, List[String]]] =
    Map(
      SolrCollection.ENCOUNTER_APHP -> Map(PATIENT_COL -> List(SolrColumn.SUBJECT),
        ENCOUNTER_COL -> List(SolrColumn.ENCOUNTER),
        ENCOUNTER_DATES_COL -> List(SolrColumn.Encounter.PERIOD_START, SolrColumn.Encounter.PERIOD_END)),
      SolrCollection.MEDICATIONREQUEST_APHP -> buildMap(List(SolrColumn.Medication.PERIOD_START)),
      SolrCollection.MEDICATIONADMINISTRATION_APHP -> buildMap(List(SolrColumn.Medication.PERIOD_START)),
      SolrCollection.OBSERVATION_APHP -> buildMap(List(SolrColumn.Observation.EFFECTIVE_DATETIME)),
      SolrCollection.CONDITION_APHP -> buildMap(List(SolrColumn.Condition.RECORDED_DATE)),
      SolrCollection.PATIENT_APHP -> Map(PATIENT_COL -> List(SolrColumn.PATIENT)),
      IPP_LIST -> Map(PATIENT_COL -> List(SolrColumn.PATIENT)),
      SolrCollection.DOCUMENTREFERENCE_APHP -> buildMap(List(SolrColumn.Document.DATE)),
      SolrCollection.COMPOSITION_APHP -> buildMap(List(SolrColumn.Document.DATE)),
      SolrCollection.GROUP_APHP -> Map(PATIENT_COL -> List(SolrColumn.Group.RESOURCE_ID)),
      SolrCollection.CLAIM_APHP -> buildMap(List(SolrColumn.Claim.CREATED)),
      SolrCollection.PROCEDURE_APHP -> buildMap(List(SolrColumn.Procedure.DATE)),
      "default" -> Map(DATE_COL -> List[String](), ENCOUNTER_COL -> List[String](), ENCOUNTER_DATES_COL -> List[String]())
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
      SolrCollection.ENCOUNTER_APHP -> List(SolrColumn.Encounter.ENCOUNTER_START_DATE, SolrColumn.Encounter.ENCOUNTER_END_DATE),
      SolrCollection.CONDITION_APHP -> List(SolrColumn.Encounter.ENCOUNTER_END_DATE, SolrColumn.Encounter.ENCOUNTER_START_DATE, EVENT_DATE),
      SolrCollection.PATIENT_APHP -> List(SolrColumn.Patient.BIRTHDATE),
      SolrCollection.DOCUMENTREFERENCE_APHP -> List(EVENT_DATE, SolrColumn.Encounter.ENCOUNTER_START_DATE, SolrColumn.Encounter.ENCOUNTER_END_DATE),
      SolrCollection.COMPOSITION_APHP -> List(EVENT_DATE, SolrColumn.Encounter.ENCOUNTER_START_DATE, SolrColumn.Encounter.ENCOUNTER_END_DATE),
      SolrCollection.GROUP_APHP -> List(),
      SolrCollection.CLAIM_APHP -> List(SolrColumn.Encounter.ENCOUNTER_END_DATE, SolrColumn.Encounter.ENCOUNTER_START_DATE, EVENT_DATE),
      SolrCollection.PROCEDURE_APHP -> List(EVENT_DATE, SolrColumn.Encounter.ENCOUNTER_END_DATE, SolrColumn.Encounter.ENCOUNTER_START_DATE),
      SolrCollection.MEDICATIONADMINISTRATION_APHP -> List(EVENT_DATE, SolrColumn.Encounter.ENCOUNTER_START_DATE, SolrColumn.Encounter.ENCOUNTER_END_DATE),
      SolrCollection.MEDICATIONREQUEST_APHP -> List(EVENT_DATE, SolrColumn.Encounter.ENCOUNTER_START_DATE, SolrColumn.Encounter.ENCOUNTER_END_DATE),
      SolrCollection.OBSERVATION_APHP -> List(EVENT_DATE, SolrColumn.Encounter.ENCOUNTER_START_DATE, SolrColumn.Encounter.ENCOUNTER_END_DATE),
      IPP_LIST -> List[String](),
      "default" -> List(SolrColumn.Encounter.ENCOUNTER_START_DATE, SolrColumn.Encounter.ENCOUNTER_END_DATE, EVENT_DATE)
    )

  def getSubjectColumn(id: Short, isPatient: Boolean = true): String = buildColName(id, if (isPatient) SolrColumn.PATIENT else SolrColumn.ID)

  def getEncounterColumn(id: Short): String = buildColName(id, SolrColumn.ENCOUNTER)

  def getDateColumn(id: Short): String = buildColName(id, LOCAL_DATE)

  def getEventDateColumn(id: Short): String = buildColName(id, EVENT_DATE)

  def getEncounterStartDateColumn(id: Short): String = buildColName(id, SolrColumn.Encounter.ENCOUNTER_START_DATE)

  def getEncounterEndDateColumn(id: Short): String = buildColName(id, SolrColumn.Encounter.ENCOUNTER_END_DATE)

  def getPatientBirthColumn(id: Short): String = buildColName(id, SolrColumn.Patient.PATIENT_BIRTHDATE)

}
