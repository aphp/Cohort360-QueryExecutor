package fr.aphp.id.eds.requester.query.engine

import fr.aphp.id.eds.requester.QueryColumn
import fr.aphp.id.eds.requester.QueryColumn.EVENT_DATE
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, functions => F}

class QueryBuilderUtils {
  private val logger = Logger.getLogger(this.getClass)

  val qbConfigs = new QueryBuilderConfigs

  /** Build a date column for a ressource based on date_preference.
    *
    * @param df datframe on which adding a column s"${local_id}_::_localDate${suffixe}"
    * @param suffixe used to name the local datetime column
    * @param datePreference list of date to coalesce to build the local datetime column
    * @param localId id of teh ressource. Used to name the local datetime column
    * */
  def buildLocalDateColumn(df: DataFrame,
                           localId: Short,
                           datePreference: List[String],
                           collection: String,
                           suffixe: String = ""): DataFrame = {
    def filterIrrelevantDatePreference(): List[String] = {
      datePreference.filter(x =>
        x match {
          case EVENT_DATE => qbConfigs.listCollectionWithEventDatetimeFields.contains(collection)
          case QueryColumn.ENCOUNTER_START_DATE | QueryColumn.ENCOUNTER_END_DATE =>
            qbConfigs.listCollectionWithEncounterFields.contains(collection)
          case _ => false
      })
    }

    val cleanedDatePreference: List[String] = filterIrrelevantDatePreference()
    val newDateColumnName = s"${qbConfigs.getDateColumn(localId)}$suffixe"

    if (logger.isDebugEnabled) {
      logger.debug(
        s"BUILD LOCAL DATETIME COLUMN: crterionId=$localId, datePreference=$cleanedDatePreference, " +
          s"df.columns=${df.columns.toList}, collection=$collection, newDateColumnName=$newDateColumnName, df.head=${df.head(10).toList}")
    }

    cleanedDatePreference.size match {
      case 0 =>
        throw new Exception(
          s"impossible to build a datetime column with date_preference=$cleanedDatePreference " +
            s"for subrequest id=$localId because there is no datetime information for encounter and event")
      case 1 =>
        val dp = cleanedDatePreference.head
        df.withColumn(newDateColumnName, F.col(qbConfigs.buildColName(localId,dp)))
      case _ =>
        df.withColumn(newDateColumnName,
                      F.coalesce(cleanedDatePreference.map(x => F.col(qbConfigs.buildColName(localId,x))): _*))

    }
  }

  /**
   * Remove unecessary columns and deduplicate dataframe (if needed)
   * @param groupDataFrame the dataframe to clean
   * @param keepAll weither or not to keep all columns
   * @param selectedColumns the list of columns to keep
   * @param deduplicationCol the column to use for deduplication
   * @return the cleaned dataframe
   */
  def cleanDataFrame(groupDataFrame: DataFrame, keepAll: Boolean, selectedColumns: List[String], deduplicationCol: String): DataFrame = {
    if (keepAll) {
      groupDataFrame
    } else {
      groupDataFrame.select(selectedColumns.map(F.col): _*).dropDuplicates(deduplicationCol)
    }
  }
}