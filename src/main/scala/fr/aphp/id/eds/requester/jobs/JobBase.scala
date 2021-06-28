package fr.aphp.id.eds.requester.jobs

import com.typesafe.config.Config
import fr.aphp.id.eds.requester.SparkJobParameter
import org.apache.spark.sql.SparkSession


case class JobEnv(jobId: String, contextConfig: Config) {
}

trait JobBase {
  type JobData
  type JobOutput

  def runJob(spark: SparkSession, runtime: JobEnv, data: SparkJobParameter): JobOutput
}

