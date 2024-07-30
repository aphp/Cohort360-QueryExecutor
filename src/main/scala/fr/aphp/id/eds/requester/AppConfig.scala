package fr.aphp.id.eds.requester

import com.typesafe.config.{Config, ConfigFactory}

case class SparkConfig(
    master: String,
    driverPort: Int,
    driverHost: String,
    executorMemory: String
)

case class SolrConfig(
    zk: String,
    maxTry: Int,
    rows: Int,
    commitWithin: Int,
    authFile: String
)

case class FhirServerConfig(
    url: String,
    cohortUrl: Option[String],
    accessToken: Option[String]
)

case class PGConfig(
    host: String,
    port: String,
    database: String,
    schema: String,
    user: String,
    cohortConfig: CohortConfig
)

case class JobConfig(
    threads: Int
)

case class ServerConfig(
    port: Int
)

case class CohortConfig(
    cohortTableName: String,
    cohortItemsTableName: String,
    cohortProviderName: String
)

case class BusinessConfig(
    jobs: JobConfig,
    cohortCreationLimit: Int,
    enableCache: Boolean,
    queryConfig: QueryConfig
)

case class BackConfig(
    url: Option[String],
    authToken: Option[String]
)

case class QueryConfig(
    useSourcePopulation: Boolean,
    useActiveFilter: Boolean
)

class AppConfig(conf: Config) {
  val spark: SparkConfig = SparkConfig(
    conf.getString("spark.master"),
    conf.getInt("spark.driver.port"),
    conf.getString("spark.driver.host"),
    if (conf.hasPath("spark.executor.memory")) conf.getString("spark.executor.memory") else "1G"
  )
  val defaultResolver: String = conf.getString("app.defaultResolver")
  val defaultCohortCreationService: String = conf.getString("app.defaultCohortCreationService")
  val solr: Option[SolrConfig] = if (conf.hasPath("solr.zk")) {
    Some(
      SolrConfig(
        conf.getString("solr.zk"),
        conf.getInt("solr.maxTry"),
        conf.getInt("solr.rows"),
        conf.getInt("solr.commitWithin"),
        conf.getString("solr.authFile")
      ))
  } else {
    None
  }
  val fhir: Option[FhirServerConfig] = if (conf.hasPath("fhir.url")) {
    Some(
      FhirServerConfig(
        conf.getString("fhir.url"),
        if (conf.hasPath("fhir.cohortUrl")) {
          Some(conf.getString("fhir.cohortUrl"))
        } else {
          None
        },
        if (conf.hasPath("fhir.accessToken")) {
          Some(conf.getString("fhir.accessToken"))
        } else {
          None
        }
      ))
  } else { None }
  val pg: Option[PGConfig] = if (conf.hasPath("postgres.host")) {
    Some(
      PGConfig(
        conf.getString("postgres.host"),
        conf.getString("postgres.port"),
        conf.getString("postgres.database"),
        conf.getString("postgres.schema"),
        conf.getString("postgres.user"),
        CohortConfig(
          conf.getString("postgres.tables.cohortTableName"),
          conf.getString("postgres.tables.cohortTableItemsName"),
          conf.getString("postgres.options.providerName")
        )
      ))
  } else { None }
  val business: BusinessConfig = BusinessConfig(
    JobConfig(conf.getInt("app.jobs.threads")),
    conf.getInt("app.cohortCreationLimit"),
    conf.getBoolean("app.enableCache"),
    QueryConfig(conf.getBoolean("app.query.useSourcePopulation"), conf.getBoolean("app.query.useActiveFilter"))
  )
  val server: ServerConfig = ServerConfig(
    conf.getInt("app.server.port")
  )
  val back: BackConfig = BackConfig(
    if (conf.hasPath("app.back.url")) { Some(conf.getString("app.back.url")) } else {
      None
    },
    if (conf.hasPath("app.back.authToken")) { Some(conf.getString("app.back.authToken")) } else {
      None
    }
  )
}

object AppConfig {
  private val appConf: AppConfig = new AppConfig(ConfigFactory.load)
  def get: AppConfig = appConf
}
