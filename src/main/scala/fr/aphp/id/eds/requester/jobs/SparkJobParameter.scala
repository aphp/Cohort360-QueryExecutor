package fr.aphp.id.eds.requester.jobs

case class SparkJobParameter(
    cohortDefinitionName: String,
    cohortDefinitionDescription: Option[String],
    cohortDefinitionSyntax: String,
    ownerEntityId: String,
    solrRows: String = "10000",
    commitWithin: String = "10000",
    mode: String = JobType.count,
    cohortUuid: Option[String] = Option.empty,
    callbackUrl: Option[String] = Option.empty
)

object JobType extends Enumeration {
  type JobType = String
  val count = "count"
  val countAll = "count_all"
  val countWithDetails = "count_with_details"
  val create = "create"
  val purgeCache = "purge_cache"
}

object ResourceType extends Enumeration {
  type ResourceType = String
  val claim = "Claim"
  val condition = "Condition"
  val documentReference = "DocumentReference"
  val encounter = "Encounter"
  val imagingStudy = "ImagingStudy"
  val imagingSeries = "ImagingSeries"
  val medicationAdministration = "MedicationAdministration"
  val medicationRequest = "MedicationRequest"
  val observation = "Observation"
  val patient = "Patient"
  val procedure = "Procedure"

  // TODO enable all when the proper concept terminologies are up
  // -> https://gitlab.eds.aphp.fr/bigdata/terminology/-/issues/98
  val all: Seq[ResourceType] = Seq(
//    claim,
//    condition,
    documentReference,
    encounter,
//    imagingStudy,
//    imagingSeries,
//    medicationAdministration,
//    medicationRequest,
//    observation,
    patient,
//    procedure
  )
}
