package fr.aphp.id.eds.requester.server

import fr.aphp.id.eds.requester.jobs.SparkConfig
import org.apache.spark.sql.SparkSession
import org.scalatra.ScalatraServlet

class HealthController(val sparkSession: SparkSession = SparkConfig.sparkSession)
    extends ScalatraServlet {

  get("/") {
    if (sparkSession != null && !sparkSession.sparkContext.isStopped) {
      "OK"
    } else {
      halt(503, "Service Unavailable: Spark context is stopped")
    }
  }

}
