package fr.aphp.id.eds.requester.tools

import fr.aphp.id.eds.requester.{AppConfig, SolrConfig}
import fr.aphp.id.eds.requester.jobs.{JobEnv, JobType, SparkJobParameter}
import fr.aphp.id.eds.requester.query._
import fr.aphp.id.eds.requester.query.model.{BaseQuery, GroupResource, GroupResourceType, QueryParsingOptions, Request, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.{CriterionTags, QueryParser}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


trait JobUtilsService {
  def initSparkJobRequest(logger: Logger,
                          spark: SparkSession,
                          runtime: JobEnv,
                          data: SparkJobParameter)
  : (Request, Map[Short, CriterionTags], OmopTools, Boolean) = {
    logger.debug(s"Received data: ${data.toString}")

    // init db connectors
    val omopTools = getOmopTools(spark, runtime)

    // load input json into object
    val (request, criterionTagsMap) = QueryParser.parse(
      data.cohortDefinitionSyntax,
      QueryParsingOptions(withOrganizationDetails = data.mode == JobType.countWithDetails)
    )

    logger.info(s"ENTER NEW QUERY JOB : ${data.toString}. Parsed criterionIdWithTcList: $criterionTagsMap")

    (request,
      criterionTagsMap,
      omopTools,
      runtime.contextConfig.business.enableCache)
  }

  def getOmopTools(session: SparkSession, env: JobEnv): OmopTools

  def getRandomIdNotInTabooList(allTabooId: List[Short]): Short
}

object JobUtils extends JobUtilsService {

  /** Read Postgresql passthrough parameters in SJS conf file */
  override def getOmopTools(spark: SparkSession, runtime: JobEnv): OmopTools = {
    val pgHost = runtime.contextConfig.pg.host
    val pgPort = runtime.contextConfig.pg.port
    val pgDb = runtime.contextConfig.pg.database
    val pgSchema = runtime.contextConfig.pg.schema
    val pgUser = runtime.contextConfig.pg.user
    new OmopTools(
      PGTool(
        spark,
        s"jdbc:postgresql://$pgHost:$pgPort/$pgDb?user=$pgUser&currentSchema=$pgSchema,public",
        "/tmp/postgres-spark-job"
      )
    )
  }

  def getDefaultSolrFilterQuery(sourcePopulation: SourcePopulation): String = {
    val list = sourcePopulation.caresiteCohortList.get.map(x => x.toString).mkString(" ")
    s"_list:(${list}) OR ({!join from=resourceId to=_subject fromIndex=groupAphp v='groupId:(${list})' score=none method=crossCollection})"
  }
  def getDefaultSolrFilterQueryPatientAphp(sourcePopulation: SourcePopulation): String = {
    getDefaultSolrFilterQuery(sourcePopulation) +
      " AND active:true"+
      " AND -(meta.security:\"http://terminology.hl7.org/CodeSystem/v3-ActCode|NOLIST\")"
  }

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
