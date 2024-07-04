package fr.aphp.id.eds.requester.tools

import fr.aphp.id.eds.requester.cohort.CohortCreation
import fr.aphp.id.eds.requester.jobs.{JobEnv, JobType, SparkJobParameter}
import fr.aphp.id.eds.requester.query.model._
import fr.aphp.id.eds.requester.query.parser.{CriterionTags, QueryParser}
import fr.aphp.id.eds.requester.query.resolver.{ResourceResolver, ResourceResolvers}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

trait JobUtilsService {
  def initSparkJobRequest(logger: Logger,
                          spark: SparkSession,
                          runtime: JobEnv,
                          data: SparkJobParameter)
  : (Request, Map[Short, CriterionTags], Option[CohortCreation], ResourceResolver, Boolean) = {
    logger.debug(s"Received data: ${data.toString}")

    // init db connectors
    val maybeCohortCreationService = getCohortCreationService(data, spark)
    val resourceResolver = getResourceResolver(data)

    // load input json into object
    val (request, criterionTagsMap) = QueryParser.parse(
      data.cohortDefinitionSyntax,
      QueryParsingOptions(
        resourceConfig = resourceResolver.getConfig,
        withOrganizationDetails = data.mode == JobType.countWithDetails,
        useFilterSolr = data.resolver == ResourceResolvers.solr,
      )
    )

    logger.info(
      s"ENTER NEW QUERY JOB : ${data.toString}. Parsed criterionIdWithTcList: $criterionTagsMap")

    (request,
      criterionTagsMap,
      maybeCohortCreationService,
      resourceResolver,
      runtime.contextConfig.business.enableCache)
  }

  def getCohortCreationService(data: SparkJobParameter, spark: SparkSession): Option[CohortCreation]

  def getResourceResolver(data: SparkJobParameter): ResourceResolver

  def getRandomIdNotInTabooList(allTabooId: List[Short]): Short

  def getCohortHash(str: String): String = {
    import java.security.MessageDigest
    MessageDigest
      .getInstance("MD5")
      .digest(str.getBytes)
      .map("%02X".format(_))
      .mkString
  }
}

object JobUtils extends JobUtilsService {

  def getCohortCreationService(data: SparkJobParameter, spark: SparkSession): Option[CohortCreation] =
    CohortCreation.get(data.cohortCreationService)(spark)

  def getResourceResolver(data: SparkJobParameter): ResourceResolver = ResourceResolver.get(data.resolver, data.resolverOpts)

  def getRandomIdNotInTabooList(allTabooId: List[Short]): Short = {
    val rnd = new scala.util.Random
    var id: Option[Short] = None
    while (id.isEmpty) {
      val rndId: Int = -252 + rnd.nextInt(252 * 2).toShort
      if (!allTabooId.contains(rndId)) id = Some(rndId.toShort)
    }
    id.get
  }

  def addEmptyGroup(allTabooId: List[Short]): BaseQuery = {
    GroupResource(groupType = GroupResourceType.AND,
                  _id = getRandomIdNotInTabooList(allTabooId),
                  isInclusive = true,
                  criteria = List())
  }

}
