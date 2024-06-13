package fr.aphp.id.eds.requester.query.resolver

import fr.aphp.id.eds.requester.query.model.{BasicResource, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class FhirResourceResolver {
  def getSolrResponseDataFrame(resource: BasicResource,
                               criterionTags: CriterionTags,
                               sourcePopulation: SourcePopulation)(
      implicit spark: SparkSession
  ): DataFrame

  def countPatients(sourcePopulation: SourcePopulation): Long
}

