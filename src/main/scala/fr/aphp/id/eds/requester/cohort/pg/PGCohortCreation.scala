package fr.aphp.id.eds.requester.cohort.pg

import com.lucidworks.spark.util.SolrDataFrameImplicits._
import com.typesafe.scalalogging.LazyLogging
import fr.aphp.id.eds.requester.cohort.CohortCreation
import fr.aphp.id.eds.requester.jobs.ResourceType
import fr.aphp.id.eds.requester.query.model.SourcePopulation
import fr.aphp.id.eds.requester.tools.SolrTools
import fr.aphp.id.eds.requester.{AppConfig, ResultColumn}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.hl7.fhir.r4.model.ListResource.ListMode

/**
  * @param pg           pgTool obj
  */
class PGCohortCreation(pg: PGTool) extends CohortCreation with LazyLogging {
  private final val cohort_item_table_rw = AppConfig.get.pg.get.cohortConfig.cohortItemsTableName
  private final val cohort_table_rw = AppConfig.get.pg.get.cohortConfig.cohortTableName
  private final val cohort_provider_name = AppConfig.get.pg.get.cohortConfig.cohortProviderName
  private final val note_text_column_name = AppConfig.get.pg.get.cohortConfig.noteTextColumnName

  override def createCohort(cohortDefinitionName: String,
                            cohortDefinitionDescription: Option[String],
                            cohortDefinitionSyntax: String,
                            ownerEntityId: String,
                            resourceType: String,
                            baseCohortId: Option[Long],
                            mode: ListMode,
                            size: Long): Long = {
    val (indentifier_col, identifier_val) = if (baseCohortId.isDefined) {
      (" identifier,", s" ${baseCohortId.get.toString},")
    } else {
      ("", "")
    }
    val stmt =
      s"""
         |insert into ${cohort_table_rw}
         |(hash, title, ${note_text_column_name},${indentifier_col} _sourcereferenceid, source__reference, _provider, source__type, mode, status, subject__type, date, _size)
         |values (-1, ?, ?,${identifier_val} ?, ?, '$cohort_provider_name', 'Practitioner', '${mode.toCode}', '${CohortStatus.RUNNING}', ?, now(), ?)
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

  /**
    * This loads both a cohort and its definition into postgres and solr
    */
  override def updateCohort(cohortId: Long,
                            cohort: DataFrame,
                            sourcePopulation: SourcePopulation,
                            count: Long,
                            delayCohortCreation: Boolean,
                            resourceType: String): Unit = {
    try {
      uploadCount(cohortId, count)
      uploadRelationship(cohortId, sourcePopulation)

      val withDeleteField = cohort.columns.contains("deleted")
      val selectedColumns = List(
        "_itemreferenceid",
        "item__reference",
        "_provider",
        "_listid"
      ) ++ (if (withDeleteField) List("deleted") else List())

      val dataframe = cohort
        .withColumn("_listid", lit(cohortId))
        .withColumn("_provider", lit(cohort_provider_name))
        .withColumnRenamed(ResultColumn.SUBJECT, "_itemreferenceid")
        .withColumn("item__reference", concat(lit(s"${resourceType}/"), col("_itemreferenceid")))
        .select(selectedColumns.map(F.col): _*)

      uploadCohortTableToPG(dataframe, withDeleteField)

      if (!delayCohortCreation && resourceType == ResourceType.patient)
        uploadCohortTableToSolr(cohortId, dataframe, count)
    } catch {
      case e: Exception =>
        setOmopCohortStatus(cohortId, CohortStatus.ERROR)
        setOmopCohortActive(cohortId, status = false)
        throw e
    } finally {
      pg.purgeTmp()
      cohort.unpersist
    }
  }

  override def readCohortEntries(cohortId: Long)(implicit spark: SparkSession): DataFrame = {
    val stmt =
      s"""
         |select _itemreferenceid
         |from ${cohort_item_table_rw}
         |where _listid = $cohortId
         |""".stripMargin
    val baseCohort = pg.sqlExecWithResult(stmt)
    val diffs = readCohortDiffEntries(cohortId)
    val addedDiffs = diffs.filter(col("deleted").isNull || col("deleted") === false)
    val deletedDiffs = diffs.filter(col("deleted") === true)

    val result = baseCohort
      .union(addedDiffs.select("_itemreferenceid"))
      .except(deletedDiffs.select("_itemreferenceid"))

    result
  }

  private def readCohortDiffEntries(cohortId: Long): DataFrame = {
    val stmt =
      s"""
             |select date,_itemreferenceid,deleted
             |from ${cohort_item_table_rw}
             |join ${cohort_table_rw} on ${cohort_table_rw}.id = ${cohort_item_table_rw}._listid
             |where ${cohort_table_rw}.identifier___official__value = '$cohortId'
             |""".stripMargin
    pg.sqlExecWithResult(stmt)
      .select(col("date"), col("_itemreferenceid"), col("deleted"))
      .orderBy(col("date").asc)
      .groupBy(col("_itemreferenceid"))
      .agg(last(col("deleted")).as("deleted"))
  }

  private def uploadRelationship(cohortDefinitionId: Long,
                                 sourcePopulation: SourcePopulation): Unit = {
    if (sourcePopulation.cohortList.isDefined) {
      for (sc_id <- sourcePopulation.cohortList.get) {
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

  private def uploadCohortTableToPG(df: DataFrame, withDeleteField: Boolean = false): Unit = {
    require(
      (List(
        "_listid",
        "item__reference",
        "_provider",
        "_itemreferenceid"
      ) ++ (if (withDeleteField) List("deleted") else List())).toSet == df.columns.toSet,
      "cohort dataframe shall have _listid, _provider, _provider and item__reference"
    )
    pg.outputBulk(cohort_item_table_rw,
                  dfAddHash(df),
                  Some(4),
                  primaryKeys = Seq("_listid", "_itemreferenceid", "_provider"))
  }

  /**
    * Adds a hash column based on several other columns
    *
    * @param df               DataFrame
    * @param columnsToExclude List[String] the columns not to be hashed
    * @return DataFrame
    */
  private def dfAddHash(
      df: DataFrame,
      columnsToExclude: List[String] = Nil
  ): DataFrame = {
    df.withColumn(
      "hash",
      hash(
        df.columns
          .filter(x => !columnsToExclude.contains(x))
          .map(x => col("`" + x + "`")): _*
      )
    )

  }

  private def uploadCohortTableToSolr(cohortDefinitionId: Long,
                                      df: DataFrame,
                                      count: Long): Unit = {
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
