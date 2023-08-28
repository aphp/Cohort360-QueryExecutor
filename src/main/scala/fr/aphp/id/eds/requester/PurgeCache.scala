/**
  * This file is part of SPARK-OMOP.
  *
  * SPARK-OMOP is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  * SPARK-OMOP is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License
  * along with SPARK-OMOP.  If not, see <https://www.gnu.org/licenses/>.
  */
package fr.aphp.id.eds.requester

import fr.aphp.id.eds.requester.jobs.{JobBase, JobEnv, JobExecutionStatus, SparkJobParameter}
import fr.aphp.id.eds.requester.tools.SparkTools
import org.apache.spark.sql.SparkSession

object PurgeCache extends JobBase {
  type JobData = String

  override def runJob(spark: SparkSession,
                      runtime: JobEnv,
                      data: SparkJobParameter): Map[String, String] =
    try {
      SparkTools.purgeCached(spark, None, None)
      Map("request_job_status" -> JobExecutionStatus.FINISHED, "message" -> "SJS purge cache")
    } catch {
      case _: Exception => Map("status" -> JobExecutionStatus.ERROR, "message" -> "FAILED: SJS purge caches")
    }

}
