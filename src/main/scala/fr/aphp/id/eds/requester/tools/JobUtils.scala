package fr.aphp.id.eds.requester.tools

import fr.aphp.id.eds.requester.{AppConfig, FhirServerConfig, PGConfig}
import fr.aphp.id.eds.requester.cohort.CohortCreationService
import fr.aphp.id.eds.requester.cohort.fhir.FhirCohortCreationService
import fr.aphp.id.eds.requester.cohort.pg.{PGCohortCreationService, PGTool}
import fr.aphp.id.eds.requester.jobs.{JobEnv, JobType, SparkJobParameter}
import fr.aphp.id.eds.requester.query.model._
import fr.aphp.id.eds.requester.query.parser.{CriterionTags, QueryParser}
import fr.aphp.id.eds.requester.query.resolver.rest.DefaultRestFhirClient
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

trait JobUtilsService {
  def initSparkJobRequest(logger: Logger,
                          spark: SparkSession,
                          runtime: JobEnv,
                          data: SparkJobParameter)
    : (Request, Map[Short, CriterionTags], Option[CohortCreationService], Boolean) = {
    logger.debug(s"Received data: ${data.toString}")

    // init db connectors
    val maybeCohortCreationService = getCohortCreationService(spark, data)

    // load input json into object
    val (request, criterionTagsMap) = QueryParser.parse(
      data.cohortDefinitionSyntax,
      QueryParsingOptions(withOrganizationDetails = data.mode == JobType.countWithDetails)
    )

    logger.info(
      s"ENTER NEW QUERY JOB : ${data.toString}. Parsed criterionIdWithTcList: $criterionTagsMap")

    (request,
     criterionTagsMap,
     maybeCohortCreationService,
     runtime.contextConfig.business.enableCache)
  }

  def getCohortCreationService(session: SparkSession,
                               data: SparkJobParameter): Option[CohortCreationService]

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

  override def getCohortCreationService(spark: SparkSession,
                                        data: SparkJobParameter): Option[CohortCreationService] = {
    data.cohortCreationService.getOrElse(AppConfig.get.defaultCohortCreationService) match {
      case "pg"   => getPGCohortCreationService(spark, AppConfig.get.pg)
      case "fhir" => getFhirCohortCreationService(AppConfig.get.fhir)
      case _      => None
    }
  }

  private def getFhirCohortCreationService(
      optFhirConfig: Option[FhirServerConfig]): Option[FhirCohortCreationService] = {
    if (optFhirConfig.isEmpty) {
      return None
    }
    val fhirConfig = optFhirConfig.get
    Some(
      new FhirCohortCreationService(
        new DefaultRestFhirClient(fhirConfig, cohortServer = true)
      ))
  }

  private def getPGCohortCreationService(
      sparkSession: SparkSession,
      optPgConfig: Option[PGConfig]): Option[PGCohortCreationService] = {
    if (optPgConfig.isEmpty) {
      return None
    }
    val pgConfig = optPgConfig.get
    Some(
      new PGCohortCreationService(
        PGTool(
          sparkSession,
          s"jdbc:postgresql://${pgConfig.host}:${pgConfig.port}/${pgConfig.database}?user=${pgConfig.user}&currentSchema=${pgConfig.schema},public",
          "/tmp/postgres-spark-job"
        )
      ))
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
