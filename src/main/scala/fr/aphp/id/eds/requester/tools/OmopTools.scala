package fr.aphp.id.eds.requester.tools

import com.lucidworks.spark.util.SolrDataFrameImplicits._
import com.typesafe.scalalogging.LazyLogging
import fr.aphp.id.eds.requester.query.SourcePopulation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, functions => F}

/**
  * @param pg           pgTool obj
  * @param solrOptions  Solr configs dictionary
  * @todo use of parametrized queries instead of scala string which is not securized
  */
class OmopTools(pg: PGTool, solrOptions: Map[String, String]) extends LazyLogging {

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
      ownerEntityId: String
  ): Long = {
    val cohortHash = "DEPRECATED"
    val stmt =
      s"""
         |insert into cohort_definition
         |(hash, cohort_definition_name, cohort_definition_description, cohort_definition_syntax, cohort_hash, owner_entity_id, cohort_initiation_datetime, cohort_active, subject_concept_id, owner_domain_id)
         |values (-1, ?, ?, ?, ?, ? , now(), false, 56, 'Provider')
         |returning cohort_definition_id
         |""".stripMargin
    val result = pg
      .sqlExecWithResult(
        stmt,
        List(
          cohortDefinitionName,
          cohortDefinitionDescription,
          cohortDefinitionSyntax,
          cohortHash,
          ownerEntityId
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
                   delayCohortCreation: Boolean): Unit = {
    try {
      uploadCount(cohortDefinitionId, count)
      uploadRelationship(cohortDefinitionId, sourcePopulation)

      val dataframe = cohort
        .withColumn("cohort_definition_id", lit(cohortDefinitionId))
        .withColumn("cohort_type_source_value", lit("QueryBuilder"))
        .select(F.col("subject_id"),
          F.col("cohort_type_source_value"),
          F.col("cohort_definition_id"))

      uploadCohortTableToPG(dataframe)

      if (!delayCohortCreation) uploadCohortTableToSolr(cohortDefinitionId, dataframe, count)
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
               |values (-1, now(), now(), 1147323, ?, 1147323, ?, $relationship_concept_id, 'Cohort360')
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
         |update cohort_definition
         |set cohort_size = $count
         |where cohort_definition_id = $cohortDefinitionId
         |""".stripMargin
    pg.sqlExec(stmt)
  }

  private def setOmopCohortStatus(
      cohortDefinitionId: Long,
      status: CohortStatus.Value
  ): Unit = {
    val stmt =
      s"""
         |update cohort_definition
         |set cohort_status = '$status'
         |where cohort_definition_id = $cohortDefinitionId
         |""".stripMargin
    pg.sqlExec(stmt)
  }

  private def setOmopCohortActive(
      cohortDefinitionId: Long,
      status: Boolean
  ): Unit = {
    val stmt =
      s"""
         |update cohort_definition
         |set cohort_active = $status, valid_start_datetime = now()
         |where cohort_definition_id = $cohortDefinitionId
         |""".stripMargin
    pg.sqlExec(stmt)
  }

  private def uploadCohortTableToPG(df: DataFrame): Unit = {
    require(
      List(
        "cohort_definition_id",
        "subject_id",
        "cohort_type_source_value"
      ).toSet == df.columns.toSet,
      "cohort dataframe shall have cohort_definition_id and subject_id"
    )
    pg.outputBulk("cohort_query_builder", SparkTools.dfAddHash(df), Some(4))
  }

  private def uploadCohortTableToSolr(cohortDefinitionId: Long, df: DataFrame, count: Long): Unit = {
    // Change in the dataframe are not saved, its purpose is only to format the dataframe for Solr
    df.withColumn(
      "id",
      concat(col("cohort_definition_id"), lit("_"), col("subject_id"))
    )
      .withColumnRenamed("cohort_definition_id", "groupId")
      .withColumnRenamed("subject_id", "resourceId")
      .withColumn("_lastUpdated", current_timestamp())
      .write
      // Give solr connection configuration to the dataframe
      .options(solrOptions ++ Map("collection" -> "groupAphp"))
      // Save into Solr
      .solr("groupAphp")

    // check that all replicates of "groupAphp" are update
    SolrTools.checkReplications(cohortDefinitionId, solrOptions, count)
  }

}

object CohortStatus extends Enumeration {
  type status = Value
  val ERROR: CohortStatus.Value = Value("error")
  val RUNNING: CohortStatus.Value = Value("running")
  val FINISHED: CohortStatus.Value = Value("finished")
}
