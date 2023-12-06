package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.ResultColumn
import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.tools.OmopTools
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => F}

trait QueryBuilder {

      def processRequest(implicit spark: SparkSession,
                            solrConf: Map[String, String],
                            request: Request,
                            criterionTagsMap: Map[Short, CriterionTags],
                            omopTools: OmopTools,
                            ownerEntityId: String,
                            cacheEnabled: Boolean,
                            withOrganizationDetails: Boolean,
                            recursiveQueryBuilder: QueryBuilderGroup = new QueryBuilderGroup()
                          ): DataFrame
}

object QueryBuilder extends QueryBuilder {

  val qbUtils = new QueryBuilderConfigs()

  /** Computes the resulting df of a request.
    *
    * @param request the request object
    * @param solrConf solr configs extracted from SJS config
    * @param criterionTagsMap list of criterion id concerned by tc
    * @param omopTools instance of object to interact with cache
    * @param ownerEntityId the id of the user to name the cache
    * */
  override def processRequest(implicit spark: SparkSession,
                     solrConf: Map[String, String],
                     request: Request,
                     criterionTagsMap: Map[Short, CriterionTags],
                     omopTools: OmopTools,
                     ownerEntityId: String,
                     cacheEnabled: Boolean,
                     withOrganizationDetails: Boolean,
                     recursiveQueryBuilder: QueryBuilderGroup = new QueryBuilderGroup()
                    ): DataFrame = {

    val cohortDataFrame = recursiveQueryBuilder.processSubrequest(
      spark,
      solrConf,
      request.request.get,
      request.sourcePopulation,
      criterionTagsMap,
      omopTools = omopTools,
      ownerEntityId = ownerEntityId,
      enableCurrentGroupCache = false,
      cacheEnabled
    )

    // need to rename final column to uniformize any results being processed after (count and/or upload in databases).
    val renamedDf = cohortDataFrame
      .withColumnRenamed(qbUtils.getSubjectColumn(request.request.get.i, isPatient = request.resourceType == ResourceType.patient), ResultColumn.SUBJECT)
      .withColumnRenamed(qbUtils.getOrganizationsColumn(request.request.get.i), ResultColumn.ORGANIZATIONS)
    if (withOrganizationDetails) {
      renamedDf.select(F.col(ResultColumn.SUBJECT), F.col(ResultColumn.ORGANIZATIONS))
    } else {
      renamedDf.select(F.col(ResultColumn.SUBJECT))
    }
  }
}
