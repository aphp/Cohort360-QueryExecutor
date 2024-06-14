package fr.aphp.id.eds.requester.tools

import com.lucidworks.spark.util.SolrDataFrameImplicits._
import com.typesafe.scalalogging.LazyLogging
import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.{AppConfig, ResultColumn}
import fr.aphp.id.eds.requester.query.model.SourcePopulation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, functions => F}

/**
  * @param pg           pgTool obj
  */
class OmopTools(pg: PGTool) extends LazyLogging {
  private final val cohort_item_table_rw = AppConfig.get.business.cohorts.cohortItemsTableName
  private final val cohort_table_rw = AppConfig.get.business.cohorts.cohortTableName
  private final val cohort_provider_name = AppConfig.get.business.cohorts.cohortProviderName

  /**
    * @param cohortDefinitionName        : The name of the cohort
    * @param cohortDefinitionDescription : the full description of the cohort
    * @param cohortDefinitionSyntax      : the full json of the cohort
    * @param ownerEntityId               : the owner of the cohort
    * @return
    */
  def getCohortDefinitionId(
      cohortDefinitionName: String,
      cohortDefinitionDescription: Option[String],
      cohortDefinitionSyntax: String,
      ownerEntityId: String,
      resourceType: String,
      size: Long
  ): Long = {
    val stmt =
      s"""
         |insert into ${cohort_table_rw}
         |(hash, title, note__text, _sourcereferenceid, source__reference, _provider, source__type, mode, status, subject__type, date, _size)
         |values (-1, ?, ?, ?, ?, '$cohort_provider_name', 'Practitioner', 'snapshot', '${CohortStatus.RUNNING}', ?, now(), ?)
         |returning id
         |""".stripMargin
    val result = pg
      .sqlExecWithResult(
        stmt,
        List(
          cohortDefinitionName,
          cohortDefinitionSyntax,
          ownerEntityId,
          s"Practitioner/${ownerEntityId}",
          resourceType,
          size
        )
      )
      .collect()
      .map(_.getLong(0))
    result(0)
  }

  def getCohortHash(str: String): String = {
    import java.security.MessageDigest
    MessageDigest
      .getInstance("MD5")
      .digest(str.getBytes)
      .map("%02X".format(_))
      .mkString
  }

  /**
    * This loads both a cohort and its definition into postgres and solr
    */
  def uploadCohort(cohortDefinitionId: Long,
                   cohort: DataFrame,
                   sourcePopulation: SourcePopulation,
                   count: Long,
                   delayCohortCreation: Boolean,
                   resourceType: String,
                  ): Unit = {
    try {
      uploadCount(cohortDefinitionId, count)
      uploadRelationship(cohortDefinitionId, sourcePopulation)

      val dataframe = cohort
        .withColumn("_listid", lit(cohortDefinitionId))
        .withColumn("_provider", lit(cohort_provider_name))
        .withColumnRenamed(ResultColumn.SUBJECT, "_itemreferenceid")
        .withColumn("item__reference", concat(lit(s"${resourceType}/"), col("_itemreferenceid")))
        .select(F.col("_itemreferenceid"),
          F.col("item__reference"),
          F.col("_provider"),
          F.col("_listid"))

      uploadCohortTableToPG(dataframe)

      if (!delayCohortCreation && resourceType == ResourceType.patient) uploadCohortTableToSolr(cohortDefinitionId, dataframe, count)
    } catch {
      case e: Exception =>
        setOmopCohortStatus(cohortDefinitionId, CohortStatus.ERROR)
        setOmopCohortActive(cohortDefinitionId, status = false)
        throw e
    } finally {
      pg.purgeTmp()
      cohort.unpersist
    }
  }

  private def uploadRelationship(cohortDefinitionId: Long, sourcePopulation: SourcePopulation): Unit = {
    if (sourcePopulation.caresiteCohortList.isDefined) {
      for (sc_id <- sourcePopulation.caresiteCohortList.get) {
        val (list_list_id, list_relationship_concept_id) =
          (List(List(cohortDefinitionId, sc_id), List(sc_id, cohortDefinitionId)),
           List(44818821, 44818823))
        for ((list_id, relationship_concept_id) <- list_list_id zip list_relationship_concept_id) {
          val stmt =
            s"""
               |insert into fact_relationship
               |(hash, insert_datetime, change_datetime, domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id, cdm_source)
               |values (-1, now(), now(), 1147323, ?, 1147323, ?, $relationship_concept_id, '$cohort_provider_name')
               |""".stripMargin
          pg.sqlExec(stmt, list_id)
        }
      }
    }
  }

  /**
    * This loads only a cohort definition into postgres.
    *
    * @param cohortDefinitionId   id of the cohort
    * @param count                nb of patient of the cohort
    */
  private def uploadCount(
      cohortDefinitionId: Long,
      count: Long
  ): Unit = {
    setOmopCohortSize(cohortDefinitionId, count)
    setOmopCohortStatus(cohortDefinitionId, CohortStatus.FINISHED)
    setOmopCohortActive(cohortDefinitionId, status = true)
  }

  private def setOmopCohortSize(
      cohortDefinitionId: Long,
      count: Long
  ): Unit = {
    val stmt =
      s"""
         |update ${cohort_table_rw}
         |set _size = $count
         |where id = $cohortDefinitionId
         |""".stripMargin
    pg.sqlExec(stmt)
  }

  private def setOmopCohortStatus(
      cohortDefinitionId: Long,
      status: CohortStatus.Value
  ): Unit = {
    val stmt =
      s"""
         |update ${cohort_table_rw}
         |set status = '$status'
         |where id = $cohortDefinitionId
         |""".stripMargin
    pg.sqlExec(stmt)
  }

  private def setOmopCohortActive(
      cohortDefinitionId: Long,
      status: Boolean
  ): Unit = {
    val mode = if (status) "working" else "snapshot"
    val stmt =
      s"""
         |update ${cohort_table_rw}
         |set mode = '$mode'
         |where id = $cohortDefinitionId
         |""".stripMargin
    pg.sqlExec(stmt)
  }

  private def uploadCohortTableToPG(df: DataFrame): Unit = {
    require(
      List(
        "_listid",
        "item__reference",
        "_provider",
        "_itemreferenceid"
      ).toSet == df.columns.toSet,
      "cohort dataframe shall have _listid, _provider, _provider and item__reference"
    )
    pg.outputBulk(cohort_item_table_rw, SparkTools.dfAddHash(df), Some(4))
  }

  private def uploadCohortTableToSolr(cohortDefinitionId: Long, df: DataFrame, count: Long): Unit = {
    val solrConf = AppConfig.get.solr
    if (solrConf.isEmpty) {
      return
    }
    val solrTools = new SolrTools(solrConf.get)
    val solrOptions = solrTools.getSolrConf
    // Change in the dataframe are not saved, its purpose is only to format the dataframe for Solr
    df.withColumn(
      "id",
      concat(col("_listid"), lit("_"), col("_itemreferenceid"))
    )
      .withColumnRenamed("_listid", "groupId")
      .withColumnRenamed("_itemreferenceid", "resourceId")
      .withColumn("_lastUpdated", current_timestamp())
      .write
      // Give solr connection configuration to the dataframe
      .options(solrOptions ++ Map("collection" -> "groupAphp"))
      // Save into Solr
      .solr("groupAphp")

    // check that all replicates of "groupAphp" are update
    solrTools.checkReplications(cohortDefinitionId, count)
  }

}

object CohortStatus extends Enumeration {
  type status = Value
  val ERROR: CohortStatus.Value = Value("entered-in-error")
  val RUNNING: CohortStatus.Value = Value("retired")
  val FINISHED: CohortStatus.Value = Value("current")
}
