package fr.aphp.id.eds.requester.query.engine

import fr.aphp.id.eds.requester.ResultColumn
import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.query.model.{CacheConfig, QueryContext, Request}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.tools.{JobUtils, JobUtilsService}
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

import scala.collection.mutable

trait QueryBuilder {

  def processRequest(implicit spark: SparkSession,
                     request: Request,
                     criterionTagsMap: Map[Short, CriterionTags],
                     stageCounts: Option[mutable.Map[Short, Long]],
                     ownerEntityId: String,
                     cacheEnabled: Boolean,
                     withOrganizationDetails: Boolean,
                     recursiveQueryBuilder: QueryBuilderGroup): DataFrame
}

class DefaultQueryBuilder(val jobUtilsService: JobUtilsService = JobUtils) extends QueryBuilder {

  /** Computes the resulting df of a request.
    *
    * @param request the request object
    * @param criterionTagsMap list of criterion id concerned by tc
    * @param ownerEntityId the id of the user to name the cache
    * */
  override def processRequest(implicit spark: SparkSession,
                              request: Request,
                              criterionTagsMap: Map[Short, CriterionTags],
                              stageCounts: Option[mutable.Map[Short, Long]],
                              ownerEntityId: String,
                              cacheEnabled: Boolean,
                              withOrganizationDetails: Boolean,
                              recursiveQueryBuilder: QueryBuilderGroup): DataFrame = {
    val (root, updatedCriteriontagsMap) = jobUtilsService.prepareRequest(request, criterionTagsMap)

    val cohortDataFrame = recursiveQueryBuilder.processSubrequest(
      root,
      updatedCriteriontagsMap,
      QueryContext(
        spark,
        request.sourcePopulation,
        recursiveQueryBuilder.qbBasicResource.querySolver.countPatients(request.sourcePopulation),
        stageCounts,
        CacheConfig(ownerEntityId, enableCurrentGroupCache = false, cacheNestedGroup = cacheEnabled)
      )
    )

    // need to rename final column to uniformize any results being processed after (count and/or upload in databases).
    val renamedDf = cohortDataFrame
      .withColumnRenamed(QueryBuilderUtils.getSubjectColumn(
                           root.i,
                           isPatient = request.resourceType == ResourceType.patient),
                         ResultColumn.SUBJECT)
      .withColumnRenamed(QueryBuilderUtils.getOrganizationsColumn(root.i),
                         ResultColumn.ORGANIZATIONS)
    val finalDf = if (withOrganizationDetails) {
      renamedDf.select(F.col(ResultColumn.SUBJECT), F.col(ResultColumn.ORGANIZATIONS))
    } else {
      renamedDf.select(F.col(ResultColumn.SUBJECT))
    }
    finalDf.dropDuplicates()
  }

}
