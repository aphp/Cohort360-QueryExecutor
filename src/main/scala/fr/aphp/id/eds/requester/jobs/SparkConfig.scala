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
  sparkConf.set("spark.driver.port", AppConfig.conf.getString("spark.driver.port"))
  sparkConf.set("spark.driver.host", AppConfig.conf.getString("spark.driver.host"))
  sparkConf.set("spark.executor.memory", if (AppConfig.conf.hasPath("spark.executor.memory")) AppConfig.conf.getString("spark.executor.memory") else "1G")
  sparkConf.set("spark.executor.extraJavaOptions", "-Dsolr.httpclient.builder.factory=org.apache.solr.client.solrj.impl.PreemptiveBasicAuthClientBuilderFactory -Dsolr.httpclient.config=solr_auth.txt")

  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .master(AppConfig.conf.getString("spark.master"))
    .getOrCreate()

}
