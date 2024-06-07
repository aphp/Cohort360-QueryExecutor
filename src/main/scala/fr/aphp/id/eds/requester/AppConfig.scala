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
    max_try: Int,
    rows: Int,
    commit_within: Int,
    auth_file: String
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
  val solr: SolrConfig = SolrConfig(
    conf.getString("solr.zk"),
    conf.getInt("solr.max_try"),
    conf.getInt("solr.rows"),
    conf.getInt("solr.commit_within"),
    conf.getString("solr.auth_file")
  )
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
