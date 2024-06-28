package fr.aphp.id.eds.requester.cohort

import fr.aphp.id.eds.requester.cohort.CohortCreationServices.CohortCreationServices
import fr.aphp.id.eds.requester.cohort.fhir.FhirCohortCreation
import fr.aphp.id.eds.requester.cohort.pg.{PGCohortCreation, PGTool}
import fr.aphp.id.eds.requester.query.model.SourcePopulation
import fr.aphp.id.eds.requester.query.resolver.rest.DefaultRestFhirClient
import fr.aphp.id.eds.requester.{AppConfig, FhirServerConfig, PGConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CohortCreation {

  /**
    * @param cohortDefinitionName        : The name of the cohort
    * @param cohortDefinitionDescription : the full description of the cohort
    * @param cohortDefinitionSyntax      : the full json of the cohort
    * @param ownerEntityId               : the owner of the cohort
    * @param resourceType                : the type of the resource
    * @param size                        : the size of the cohort
    * @return the id of the newly created cohort
    */
  def createCohort(cohortDefinitionName: String,
                   cohortDefinitionDescription: Option[String],
                   cohortDefinitionSyntax: String,
                   ownerEntityId: String,
                   resourceType: String,
                   size: Long): Long

  def updateCohort(cohortId: Long,
                   cohort: DataFrame,
                   sourcePopulation: SourcePopulation,
                   count: Long,
                   delayCohortCreation: Boolean,
                   resourceType: String): Unit

}

object CohortCreation {
  def get(resolver: CohortCreationServices)(
      implicit spark: SparkSession): Option[CohortCreation] = {
    resolver match {
      case CohortCreationServices.pg   => getPGCohortCreationService(spark, AppConfig.get.pg)
      case CohortCreationServices.fhir => getFhirCohortCreationService(AppConfig.get.fhir)
      case _                           => None
    }
  }

  private def getFhirCohortCreationService(
      optFhirConfig: Option[FhirServerConfig]): Option[FhirCohortCreation] = {
    if (optFhirConfig.isEmpty) {
      return None
    }
    val fhirConfig = optFhirConfig.get
    Some(
      new FhirCohortCreation(
        new DefaultRestFhirClient(fhirConfig, cohortServer = true)
      ))
  }

  private def getPGCohortCreationService(
      sparkSession: SparkSession,
      optPgConfig: Option[PGConfig]): Option[PGCohortCreation] = {
    if (optPgConfig.isEmpty) {
      return None
    }
    val pgConfig = optPgConfig.get
    Some(
      new PGCohortCreation(
        PGTool(
          sparkSession,
          s"jdbc:postgresql://${pgConfig.host}:${pgConfig.port}/${pgConfig.database}?user=${pgConfig.user}&currentSchema=${pgConfig.schema},public",
          "/tmp/postgres-spark-job"
        )
      ))
  }
}
