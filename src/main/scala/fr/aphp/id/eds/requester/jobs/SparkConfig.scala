package fr.aphp.id.eds.requester.jobs

import com.typesafe.config.ConfigFactory
import fr.aphp.id.eds.requester.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConfig {

  private val sparkConf = new SparkConf()
  sparkConf.setJars(Seq("postgresql.jar", "cohort-requester-libs.jar"))
  sparkConf.set("spark.scheduler.mode", "FAIR")
  sparkConf.set("spark.driver.bindAddress", "0.0.0.0")
  sparkConf.set("spark.driver.port", AppConfig.get.spark.driverPort.toString)
  sparkConf.set("spark.driver.host", AppConfig.get.spark.driverHost)
  sparkConf.set("spark.executor.memory", AppConfig.get.spark.executorMemory)
  sparkConf.set(
    "spark.executor.extraJavaOptions",
    f"-Dsolr.httpclient.builder.factory=org.apache.solr.client.solrj.impl.PreemptiveBasicAuthClientBuilderFactory -Dsolr.httpclient.config=${AppConfig.get.solr.auth_file}"
  )

  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .master(AppConfig.get.spark.master)
    .getOrCreate()

}
