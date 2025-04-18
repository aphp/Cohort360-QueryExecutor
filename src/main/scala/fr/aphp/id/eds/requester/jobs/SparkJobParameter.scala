package fr.aphp.id.eds.requester.jobs

import fr.aphp.id.eds.requester.AppConfig
import fr.aphp.id.eds.requester.cohort.CohortCreationServices.CohortCreationServices
import fr.aphp.id.eds.requester.query.resolver.ResourceResolvers.ResourceResolvers

case class SparkJobParameter(
    cohortDefinitionName: String,
    cohortDefinitionDescription: Option[String],
    cohortDefinitionSyntax: String,
    ownerEntityId: String,
    solrRows: String = "10000",
    commitWithin: String = "10000",
    mode: String = JobType.count,
    // see fr.aphp.id.eds.requester.{CountQuery, CreateQuery} for the usage of modeOptions
    modeOptions: Map[String, String] = Map.empty,
    cohortUuid: Option[String] = Option.empty,
    existingCohortId: Option[Long] = Option.empty,
    callbackPath: Option[String] = Option.empty,
    callbackUrl: Option[String] = Option.empty,
    resolver: ResourceResolvers = AppConfig.get.defaultResolver,
    resolverOpts: Map[String, String] = Map.empty,
    cohortCreationService: CohortCreationServices = AppConfig.get.defaultCohortCreationService
)

object JobType extends Enumeration {
  type JobType = String
  val count = "count"
  val countAll = "count_all"
  val countWithDetails = "count_with_details"
  val create = "create"
  val createDiff = "create_diff"
  val purgeCache = "purge_cache"
}

object ResourceType extends Enumeration {
  type ResourceType = String
  val claim = "Claim"
  val condition = "Condition"
  val documentReference = "DocumentReference"
  val encounter = "Encounter"
  val imagingStudy = "ImagingStudy"
  val medicationAdministration = "MedicationAdministration"
  val medicationRequest = "MedicationRequest"
  val observation = "Observation"
  val patient = "Patient"
  val procedure = "Procedure"
  val questionnaireResponse = "QuestionnaireResponse"

  val all: Seq[ResourceType] = Seq(
    claim,
    condition,
    documentReference,
    encounter,
    imagingStudy,
    medicationAdministration,
    medicationRequest,
    observation,
    patient,
    procedure,
    questionnaireResponse
  )
}
