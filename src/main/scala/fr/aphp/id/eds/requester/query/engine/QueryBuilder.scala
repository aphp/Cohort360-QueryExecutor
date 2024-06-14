package fr.aphp.id.eds.requester.query.engine

import fr.aphp.id.eds.requester.{FhirResource, ResultColumn}
import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.query.model.{GroupResource, GroupResourceType, Request}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.query.resolver.{FhirResourceResolverFactory, QueryElementsConfig}
import fr.aphp.id.eds.requester.tools.{JobUtils, JobUtilsService, OmopTools}
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

trait QueryBuilder {

      def processRequest(implicit spark: SparkSession,
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

  val qbUtils: QueryElementsConfig = FhirResourceResolverFactory.getDefaultConfig

  /** Computes the resulting df of a request.
    *
    * @param request the request object
    * @param criterionTagsMap list of criterion id concerned by tc
    * @param omopTools instance of object to interact with cache
    * @param ownerEntityId the id of the user to name the cache
    * */
  override def processRequest(implicit spark: SparkSession,
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
        groupType = GroupResourceType.AND,
        _id = criteriaId,
        isInclusive = true,
        criteria = List(request.request.get)
      ), criterionTagsMap ++ Map(
        criteriaId -> new CriterionTags(false,
          false,
          false,
          false,
          List[String](),
          FhirResource.PATIENT,
          List[String]())))
    } else {
      (request.request.get, criterionTagsMap)
    }


    val cohortDataFrame = recursiveQueryBuilder.processSubrequest(
      spark,
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
