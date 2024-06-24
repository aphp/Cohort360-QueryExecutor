package fr.aphp.id.eds.requester.query.resolver

import fr.aphp.id.eds.requester.query.model.{BasicResource, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Interface for services that fetch fhir resources from a data source.
 */
abstract class ResourceResolver {
  /**
   * Fetches a DataFrame of the resource specified by the BasicResource object.
   * @param resource The resource to fetch.
   * @param criterionTags The tags that specify some fetch configuration for the resource.
   * @param sourcePopulation The source population to fetch the resource from.
   * @param spark The SparkSession to use for fetching the resource.
   * @return A DataFrame of the fetched resource.
   */
  def getResourceDataFrame(resource: BasicResource,
                           criterionTags: CriterionTags,
                           sourcePopulation: SourcePopulation)(
      implicit spark: SparkSession
  ): DataFrame

  /**
   * Counts the number of patients in the source population.
   * @param sourcePopulation The source population to count the patients from.
   * @return The number of patients in the source population.
   */
  def countPatients(sourcePopulation: SourcePopulation): Long

  /**
   * Construct the default filter query for the patient.
   * @param sourcePopulation The source population to fetch the default filter query for.
   * @return The default filter query for the patient.
   */
  def getDefaultFilterQueryPatient(sourcePopulation: SourcePopulation): String

  /**
   * Returns the resource configuration for the resource resolver.
   * @return The resource configuration for the resource resolver.
   */
  def getConfig: ResourceConfig
}

