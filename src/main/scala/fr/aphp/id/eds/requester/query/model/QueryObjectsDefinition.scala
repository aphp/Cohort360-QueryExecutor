package fr.aphp.id.eds.requester.query.model

import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.query.resolver.ResourceConfig
import org.apache.spark.sql.DataFrame

abstract class BaseQuery(val _type: String, _id: Short, isInclusive: Boolean) {
  val i: Short = _id
  val IsInclusive: Boolean = isInclusive
}

case class UniqueFieldConstraint(
    name: Option[String],
    operator: String,
    n: Int
)

case class Occurrence(n: Int,
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
                         encounterDateRange: Option[DateRange] = None,
                         uniqueFields: Option[List[UniqueFieldConstraint]] = None,
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

case class GroupResource(groupType: String,
                         _id: Short,
                         isInclusive: Boolean,
                         criteria: List[BaseQuery],
                         temporalConstraints: Option[List[TemporalConstraint]] = None,
                         nAmongMOptions: Option[Occurrence] = None)
    extends BaseQuery(groupType, _id, isInclusive) {
  override def toString: String = getClass.getName + "@" + Integer.toHexString(hashCode)
}

object GroupResourceType {
  final val AND = "andGroup"
  final val OR = "orGroup"
  final val N_AMONG_M = "nAmongM"
}

object TemporalConstraintType {
  final val SAME_ENCOUNTER = "sameEncounter"
  final val DIFFERENT_ENCOUNTER = "differentEncounter"
  final val DIRECT_CHRONOLOGICAL_ORDERING = "directChronologicalOrdering"
  final val SAME_EPISODE_OF_CARE = "sameEpisodeOfCare"
}

case class TemporalConstraintDuration(
    years: Option[Int],
    months: Option[Int],
    days: Option[Int],
    hours: Option[Int],
    minutes: Option[Int],
    seconds: Option[Int]
)

case class SourcePopulation(cohortList: Option[List[Int]],
                            count: Option[Long] = None,
                            df: Option[DataFrame] = None)

case class Request(_type: String = "request",
                   sourcePopulation: SourcePopulation,
                   request: Option[BaseQuery],
                   resourceType: String = ResourceType.patient)

case class QueryParsingOptions(resourceConfig: ResourceConfig,
                               withOrganizationDetails: Boolean = false,
                               useFilterSolr: Boolean = true)

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
