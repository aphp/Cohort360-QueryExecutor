package fr.aphp.id.eds.requester.tools

import fr.aphp.id.eds.requester.jobs.{JobEnv, SparkJobParameter}
import fr.aphp.id.eds.requester.query._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


trait JobUtilsService {
  def initSparkJobRequest(logger: Logger,
                          spark: SparkSession,
                          runtime: JobEnv,
                          data: SparkJobParameter)
  : (Request, Map[Short, CriterionTags], Map[String, String], OmopTools, Boolean) = {
    logger.debug(s"Received data: ${data.toString}")

    // init db connectors
    val solrConf: Map[String, String] = getSolrConf(runtime)
    val omopTools = getOmopTools(spark, runtime, solrConf)

    // load input json into object
    val (request, criterionTagsMap) = QueryParser.parse(data.cohortDefinitionSyntax)

    logger.info(s"ENTER NEW QUERY JOB : ${data.toString}. Parsed criterionIdWithTcList: $criterionTagsMap")

    (request,
      criterionTagsMap,
      solrConf,
      omopTools,
      runtime.contextConfig.getBoolean("app.enableCache"))
  }

  def getOmopTools(session: SparkSession, env: JobEnv, stringToString: Map[String, String]): OmopTools

  def getSolrConf(env: JobEnv): Map[String, String]
}

object JobUtils extends JobUtilsService {

  /** Read Postgresql passthrough parameters in SJS conf file */
  override def getOmopTools(spark: SparkSession, runtime: JobEnv, solrConf: Map[String, String]): OmopTools = {
    val pgHost =
      runtime.contextConfig.getString(
        "postgres.host"
      )
    val pgPort =
      runtime.contextConfig.getString(
        "postgres.port"
      )
    val pgDb =
      runtime.contextConfig.getString(
        "postgres.database"
      )
    val pgSchema =
      runtime.contextConfig.getString(
        "postgres.schema"
      )
    val pgUser =
      runtime.contextConfig.getString(
        "postgres.user"
      )
    new OmopTools(
      PGTool(
        spark,
        s"jdbc:postgresql://$pgHost:$pgPort/$pgDb?user=$pgUser&currentSchema=$pgSchema,public",
        "/tmp/postgres-spark-job"
      ),
      solrConf
    )

  }

  /** Read SolR passthrough parameters in SJS conf file */
  override def getSolrConf(runtime: JobEnv): Map[String, String] = {
    val zkHost = runtime.contextConfig.getString("solr.zk")
    val maxSolrTry = runtime.contextConfig.getString("solr.max_try")
    val rows = runtime.contextConfig.getString("solr.rows")
    val commitWithin = runtime.contextConfig.getString("solr.commit_within")

    val options = Map(
      "zkhost" -> zkHost,
      "batch_size" -> "10000",
      "timezone_id" -> "Europe/Paris",
      "request_handler" -> "/export",
      "rows" -> rows,
      "commit_within" -> commitWithin,
      "max_solr_try" -> maxSolrTry
    )
    options
  }

  def getDefaultSolrFilterQuery(sourcePopulation: SourcePopulation): String = {
    s"_list:(${sourcePopulation.caresiteCohortList.get.map(x => x.toString).mkString(" ")})"
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
    GroupResource(_type = "andGroup",
                  _id = getRandomIdNotInTabooList(allTabooId),
                  isInclusive = true,
                  criteria = List())
  }

}
