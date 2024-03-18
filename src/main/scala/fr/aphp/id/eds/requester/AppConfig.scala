package fr.aphp.id.eds.requester

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  val conf: Config = ConfigFactory.load
  val djangoUrl: String = if (conf.hasPath("app.back.url")) {
    conf.getString("app.back.url")
  } else {
    throw new RuntimeException("No Django URL provided")
  }
  val backAuthToken: String = if (conf.hasPath("app.back.authToken")) {
    conf.getString("app.back.authToken")
  } else {
    throw new RuntimeException("No token provided")
  }
  val cohortTableName: String = if (conf.hasPath("app.cohortTableName")) {
    conf.getString("app.cohortTableName")
  } else {
    "list_cohort360"
  }
  val cohortItemsTableName: String = if (conf.hasPath("app.cohortTableItemsName")) {
    conf.getString("app.cohortTableItemsName")
  } else {
    "list__entry_cohort360"
  }
}
