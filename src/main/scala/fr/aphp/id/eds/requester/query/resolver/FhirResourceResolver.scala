package fr.aphp.id.eds.requester.query.resolver

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class FhirResourceResolver {
  def getSolrResponseDataFrame(resourceType: String,
                               requestedFields: String,
                               requestFilter: String)(
      implicit spark: SparkSession,
      resourceId: Short = -1,
  ): DataFrame

}


