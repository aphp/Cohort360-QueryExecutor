package fr.aphp.id.eds.requester

import com.typesafe.config.{Config, ConfigFactory}
import fr.aphp.id.eds.requester.AppConfig.get

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
    url: String
)

case class PGConfig(
    host: String,
    port: String,
    database: String,
    schema: String,
    user: String,
)

case class JobConfig(
    threads: Int
)

case class CohortConfig(
    cohortCreationLimit: Int,
    cohortTableName: String,
    cohortItemsTableName: String,
    cohortProviderName: String
)

case class BusinessConfig(
    jobs: JobConfig,
    cohorts: CohortConfig,
    enableCache: Boolean
)

case class BackConfig(
    url: String,
    authToken: String
)

class AppConfig(conf: Config) {
  val spark: SparkConfig = SparkConfig(
    conf.getString("spark.master"),
    conf.getInt("spark.driver.port"),
    conf.getString("spark.driver.host"),
    if (conf.hasPath("spark.executor.memory")) conf.getString("spark.executor.memory") else "1G"
  )
  val defaultResolver: String = conf.getString("app.defaultResolver")
  val solr: Option[SolrConfig] = if (conf.hasPath("solr")) {
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
        conf.getString("fhir.url")
      ))
  } else { None }
  val pg: PGConfig = PGConfig(
    conf.getString("postgres.host"),
    conf.getString("postgres.port"),
    conf.getString("postgres.database"),
    conf.getString("postgres.schema"),
    conf.getString("postgres.user")
  )
  val business: BusinessConfig = BusinessConfig(
    JobConfig(conf.getInt("app.jobs.threads")),
    CohortConfig(
      conf.getInt("app.cohortCreationLimit"),
      if (conf.hasPath("app.cohortTableName")) {
        conf.getString("app.cohortTableName")
      } else {
        "list_cohort360"
      },
      if (conf.hasPath("app.cohortTableItemsName")) {
        conf.getString("app.cohortTableItemsName")
      } else {
        "list__entry_cohort360"
      },
      "Cohort360"
    ),
    conf.getBoolean("app.enableCache")
  )
  val back: BackConfig = BackConfig(
    conf.getString("app.back.url"),
    conf.getString("app.back.authToken")
  )
}

object AppConfig {
  private val appConf: AppConfig = new AppConfig(ConfigFactory.load)
  def get: AppConfig = appConf
}
