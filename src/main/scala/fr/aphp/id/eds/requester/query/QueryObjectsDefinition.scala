package fr.aphp.id.eds.requester.query

abstract class BaseQuery(_type: String, _id: Short, isInclusive: Boolean) {
  val i: Short = _id
  val IsInclusive: Boolean = isInclusive
}

case class Occurrence(n: Short,
                      operator: String,
                      sameEncounter: Option[Boolean],
                      sameDay: Option[Boolean],
                      timeDelayMin: Option[String],
                      timeDelayMax: Option[String])

case class PatientAge(
                       minAge: Option[String],
                       maxAge: Option[String],
                       datePreference: Option[List[String]], // [EVENT_DATE, SolrColumn.Encounter.ENCOUNTER_PLANNED_END_DATE, SolrColumn.Encounter.ENCOUNTER_PLANNED_START_DATE]
                       dateIsNotNull: Option[Boolean]) // true

case class DateRange(
                      minDate: Option[String],
                      maxDate: Option[String],
                      datePreference: Option[List[String]], // [EVENT_DATE, SolrColumn.Encounter.ENCOUNTER_PLANNED_END_DATE, SolrColumn.Encounter.ENCOUNTER_PLANNED_START_DATE]
                      dateIsNotNull: Option[Boolean]) // true

case class BasicResource(_id: Short,
                         isInclusive: Boolean,
                         resourceType: String,
                         filter: String,
                         occurrence: Option[Occurrence] = None,
                         patientAge: Option[PatientAge] = None,
                         dateRangeList: Option[List[DateRange]] = None,
                         encounterDateRange: Option[DateRange] = None,
                         nullAvailableFieldList: Option[List[String]] = None)
  extends BaseQuery("basic_resource", _id, isInclusive) {
  override def toString: String = getClass.getName + "@" + Integer.toHexString(hashCode)
}

case class TemporalConstraint(
                               idList: Either[String, List[Short]],
                               constraintType: String,
                               occurrenceChoice: Option[Map[Short, String]], // "any"
                               timeRelationMinDuration: Option[TemporalConstraintDuration], // None
                               timeRelationMaxDuration: Option[TemporalConstraintDuration], // None
                               datePreference: Option[Map[Short, List[String]]], // List(SolrColumn.Encounter.ENCOUNTER_PLANNED_START_DATE, SolrColumn.Encounter.ENCOUNTER_PLANNED_END_DATE, EVENT_DATE),
                               dateIsNotNull: Option[Either[Boolean, Map[Short, Boolean]]], // true
                               filteredCriteriaId: Option[Either[String, List[Short]]]) // "all"

case class GroupResource(_type: String,
                         _id: Short,
                         isInclusive: Boolean,
                         criteria: List[BaseQuery],
                         temporalConstraints: Option[List[TemporalConstraint]] = None,
                         nAmongMOptions: Option[Occurrence] = None)
  extends BaseQuery(_type, _id, isInclusive) {
  override def toString: String = getClass.getName + "@" + Integer.toHexString(hashCode)
}

object GroupResourceType {
    final val AND = "andGroup"
    final val OR = "orGroup"
    final val N_AMONG_M = "nAmongM"
}

case class TemporalConstraintDuration(
                                       years: Option[Int],
                                       months: Option[Int],
                                       days: Option[Int],
                                       hours: Option[Int],
                                       minutes: Option[Int],
                                       seconds: Option[Int]
                                     )

case class SourcePopulation(caresiteCohortList: Option[List[Int]],
                            providerCohortList: Option[List[Int]])

case class Request(_type: String = "request", sourcePopulation: SourcePopulation, request: Option[BaseQuery], resourceType: String = "Patient")

case class QueryParsingOptions(withOrganizationDetails: Boolean = false)

/** The 4 classes below correspond to json obj {_id: ..., value: ...} */
case class IdWithString(_id: Short, occurrenceChoice: String) {
  val i: Short = _id
  val value: String = occurrenceChoice
}

case class IdWithStringList(_id: Short, datePreference: List[String]) {
  val i: Short = _id
  val value: List[String] = datePreference
}

case class IdWithIdBoolean(_id: Short, idIsNotNull: Boolean) {
  val i: Short = _id
  val value: Boolean = idIsNotNull
}

case class IdWithDateBoolean(_id: Short, dateIsNotNull: Boolean) {
  val i: Short = _id
  val value: Boolean = dateIsNotNull
}
