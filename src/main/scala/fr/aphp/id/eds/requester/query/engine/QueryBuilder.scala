package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.ResultColumn
import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.query.engine.QueryBuilderConfigs
import fr.aphp.id.eds.requester.tools.{JobUtils, JobUtilsService, OmopTools}
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

class DefaultQueryBuilder(val jobUtilsService: JobUtilsService = JobUtils) extends QueryBuilder {

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
                     recursiveQueryBuilder: QueryBuilderGroup = new QueryBuilderGroup(jobUtilsService = jobUtilsService)
                    ): DataFrame = {

    // wrap the first group in a higher group is it is not inclusive
    val (root, updatedCriteriontagsMap) = if (!request.request.get.IsInclusive) {
      val criteriaId = jobUtilsService.getRandomIdNotInTabooList(List(request.request.get.i))
      (GroupResource(
        _type = GroupResourceType.AND,
        _id = criteriaId,
        isInclusive = true,
        criteria = List(request.request.get)
      ), criterionTagsMap ++ Map(
        criteriaId -> new CriterionTags(false,
          false,
          false,
          false,
          List[String](),
          "patientAphp",
          List[String]())))
    } else {
      (request.request.get, criterionTagsMap)
    }


    val cohortDataFrame = recursiveQueryBuilder.processSubrequest(
      spark,
      solrConf,
      root,
      request.sourcePopulation,
      updatedCriteriontagsMap,
      omopTools = omopTools,
      ownerEntityId = ownerEntityId,
      enableCurrentGroupCache = false,
      cacheEnabled
    )

    // need to rename final column to uniformize any results being processed after (count and/or upload in databases).
    val renamedDf = cohortDataFrame
      .withColumnRenamed(qbUtils.getSubjectColumn(root.i, isPatient = request.resourceType == ResourceType.patient), ResultColumn.SUBJECT)
      .withColumnRenamed(qbUtils.getOrganizationsColumn(root.i), ResultColumn.ORGANIZATIONS)
    if (withOrganizationDetails) {
      renamedDf.select(F.col(ResultColumn.SUBJECT), F.col(ResultColumn.ORGANIZATIONS))
    } else {
      renamedDf.select(F.col(ResultColumn.SUBJECT))
    }
  }
}
