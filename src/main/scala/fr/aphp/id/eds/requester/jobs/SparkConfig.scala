package fr.aphp.id.eds.requester.jobs

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConfig {

  val sparkConf = new SparkConf()

  sparkConf.setJars(Seq("postgresql.jar"))

  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .master("local[*]")
    .getOrCreate()

}
