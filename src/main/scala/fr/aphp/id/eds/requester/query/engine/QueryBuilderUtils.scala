package fr.aphp.id.eds.requester.query.engine

import fr.aphp.id.eds.requester.QueryColumn.{EVENT_DATE, LOCAL_DATE}
import fr.aphp.id.eds.requester.query.resolver.ResourceConfig
import fr.aphp.id.eds.requester.{FhirResource, QueryColumn}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, functions => F}

object QueryBuilderUtils {

  def buildColName(id: Short, colName: String): String = {
    s"${id}_::_$colName"
  }

  def getSubjectColumn(id: Short, isPatient: Boolean = true): String =
    buildColName(id, if (isPatient) QueryColumn.PATIENT else QueryColumn.ID)

  def getEncounterColumn(id: Short): String = buildColName(id, QueryColumn.ENCOUNTER)

  def getEpisodeOfCareColumn(id: Short): String = buildColName(id, QueryColumn.EPISODE_OF_CARE)

  def getDateColumn(id: Short): String = buildColName(id, LOCAL_DATE)

  def getEventDateColumn(id: Short): String = buildColName(id, EVENT_DATE)

  def getOrganizationsColumn(id: Short): String = buildColName(id, QueryColumn.ORGANIZATIONS)

  def getEncounterStartDateColumn(id: Short): String =
    buildColName(id, QueryColumn.ENCOUNTER_START_DATE)

  def getEncounterEndDateColumn(id: Short): String =
    buildColName(id, QueryColumn.ENCOUNTER_END_DATE)

  def getPatientBirthColumn(id: Short): String = buildColName(id, QueryColumn.PATIENT_BIRTHDATE)

  val defaultDatePreferencePerCollection: Map[String, List[String]] =
    Map[String, List[String]](
      FhirResource.ENCOUNTER -> List(QueryColumn.ENCOUNTER_START_DATE,
                                     QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.CONDITION -> List(QueryColumn.ENCOUNTER_END_DATE,
                                     QueryColumn.ENCOUNTER_START_DATE,
                                     EVENT_DATE),
      FhirResource.PATIENT -> List(QueryColumn.PATIENT_BIRTHDATE),
      FhirResource.DOCUMENT_REFERENCE -> List(EVENT_DATE,
                                              QueryColumn.ENCOUNTER_START_DATE,
                                              QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.COMPOSITION -> List(EVENT_DATE,
                                       QueryColumn.ENCOUNTER_START_DATE,
                                       QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.GROUP -> List(),
      FhirResource.CLAIM -> List(QueryColumn.ENCOUNTER_END_DATE,
                                 QueryColumn.ENCOUNTER_START_DATE,
                                 EVENT_DATE),
      FhirResource.PROCEDURE -> List(EVENT_DATE,
                                     QueryColumn.ENCOUNTER_END_DATE,
                                     QueryColumn.ENCOUNTER_START_DATE),
      FhirResource.MEDICATION_ADMINISTRATION -> List(EVENT_DATE,
                                                     QueryColumn.ENCOUNTER_START_DATE,
                                                     QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.MEDICATION_REQUEST -> List(EVENT_DATE,
                                              QueryColumn.ENCOUNTER_START_DATE,
                                              QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.OBSERVATION -> List(EVENT_DATE,
                                       QueryColumn.ENCOUNTER_START_DATE,
                                       QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.IMAGING_STUDY -> List(EVENT_DATE,
                                         QueryColumn.ENCOUNTER_START_DATE,
                                         QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.QUESTIONNAIRE_RESPONSE -> List(EVENT_DATE,
                                                  QueryColumn.ENCOUNTER_START_DATE,
                                                  QueryColumn.ENCOUNTER_END_DATE),
      FhirResource.UNKNOWN -> List(QueryColumn.ENCOUNTER_START_DATE,
                                   QueryColumn.ENCOUNTER_END_DATE,
                                   EVENT_DATE)
    )
}

class QueryBuilderUtils(val qbConfigs: ResourceConfig) {
  private val logger = Logger.getLogger(this.getClass)

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
          case EVENT_DATE => listCollectionWithEventDatetimeFields.contains(collection)
          case QueryColumn.ENCOUNTER_START_DATE | QueryColumn.ENCOUNTER_END_DATE =>
            listCollectionWithEncounterFields.contains(collection)
          case _ => false
      })
    }

    val cleanedDatePreference: List[String] = filterIrrelevantDatePreference()
    val newDateColumnName = s"${QueryBuilderUtils.getDateColumn(localId)}$suffixe"

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
        df.withColumn(newDateColumnName, F.col(QueryBuilderUtils.buildColName(localId, dp)))
      case _ =>
        df.withColumn(newDateColumnName,
                      F.coalesce(cleanedDatePreference.map(x =>
                        F.col(QueryBuilderUtils.buildColName(localId, x))): _*))

    }
  }

  private def listCollectionWithEventDatetimeFields: List[String] =
    qbConfigs.requestKeyPerCollectionMap
      .filter(el => el._2.contains(QueryColumn.EVENT_DATE))
      .keys
      .toList

  private def listCollectionWithEncounterFields: List[String] =
    qbConfigs.requestKeyPerCollectionMap
      .filter(el => el._2.contains(QueryColumn.ENCOUNTER) && el._2(QueryColumn.ENCOUNTER).nonEmpty)
      .keys
      .toList

  /**
    * Remove unecessary columns and deduplicate dataframe (if needed)
    * @param groupDataFrame the dataframe to clean
    * @param keepAll weither or not to keep all columns
    * @param selectedColumns the list of columns to keep
    * @param deduplicationCol the column to use for deduplication
    * @return the cleaned dataframe
    */
  def cleanDataFrame(groupDataFrame: DataFrame,
                     keepAll: Boolean,
                     selectedColumns: List[String],
                     deduplicationCol: String): DataFrame = {
    if (keepAll) {
      groupDataFrame
    } else {
      groupDataFrame.select(selectedColumns.map(F.col): _*).dropDuplicates(deduplicationCol)
    }
  }
}
