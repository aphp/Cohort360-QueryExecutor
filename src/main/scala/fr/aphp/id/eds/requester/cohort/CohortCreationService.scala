package fr.aphp.id.eds.requester.cohort

import fr.aphp.id.eds.requester.AppConfig
import fr.aphp.id.eds.requester.cohort.pg.{PGCohortCreationService, PGTool}
import fr.aphp.id.eds.requester.jobs.JobEnv
import fr.aphp.id.eds.requester.query.model.SourcePopulation
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CohortCreationService {
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

  def updateCohort(cohortId: Long, cohort: DataFrame,
                   sourcePopulation: SourcePopulation,
                   count: Long,
                   delayCohortCreation: Boolean,
                   resourceType: String): Unit

}
