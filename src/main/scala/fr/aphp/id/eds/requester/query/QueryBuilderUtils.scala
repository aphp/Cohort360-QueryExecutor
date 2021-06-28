package fr.aphp.id.eds.requester.query

import fr.aphp.id.eds.requester.{EVENT_DATE, SolrColumn}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{MapType, ShortType, StringType, StructType}
import org.apache.spark.sql.{functions => F}

import scala.collection.mutable.ListBuffer

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
          case SolrColumn.Encounter.ENCOUNTER_START_DATE | SolrColumn.Encounter.ENCOUNTER_END_DATE =>
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

  /** Get Dict_tags in cache (stored as df) and recover df */
  def convertDfTagsToDict(df: DataFrame): Map[Short, CriterionTags] = {
    def convertVal(s: String): Any =
      if (s == "true") true else if (s == "false") false else s
    if (logger.isDebugEnabled) logger.debug(s"convertDfTagsToDict with df=$df")
    val dict = df
      .select(F.col("dict"))
      .head()
      .getMap[Short, Map[String, String]](0)
      .toMap
    val convertDict: Map[Short, CriterionTags] = dict.map(
      x =>
        x._1 -> new CriterionTags(
          isDateTimeAvailable = convertVal(x._2("isDateTimeAvailable")).asInstanceOf[Boolean],
          isEncounterAvailable = convertVal(x._2("isEncounterAvailable")).asInstanceOf[Boolean],
          isInTemporalConstraint = convertVal(x._2("isInTemporalConstraint")).asInstanceOf[Boolean],
          temporalConstraintTypeList =
            convertVal(x._2("temporalConstraintTypeList")).asInstanceOf[String].split(",").toList,
          resourceType = convertVal(x._2("resourceType")).asInstanceOf[String],
          requiredSolrFieldList =
            convertVal(x._2("requiredSolrFieldList")).asInstanceOf[String].split(",").toList
      ))
    if (logger.isDebugEnabled)
      logger.debug(s"convertDfTagsToDict outputs convert_dict=${convertDict.toString()}")
    convertDict
  }

  // do not delete the following import
//  import scala.collection.JavaConversions._

  /** Store a map in cache as df */
//  def convertDictTagsToDf(implicit spark: SparkSession,
//                          dict: Map[Short, CriterionTags]): DataFrame = {
//    val convert_dict: Map[Short, Map[String, String]] =
//      dict.map(
//        x =>
//          x._1 -> Map[String, String](
//            "isDateTimeAvailable" -> x._2.isDateTimeAvailable.toString,
//            "isEncounterAvailable" -> x._2.isEncounterAvailable.toString,
//            "resourceType" -> x._2.resourceType,
//            "isInTemporalConstraint" -> x._2.isInTemporalConstraint.toString,
//            "temporalConstraintTypeList" -> x._2.temporalConstraintTypeList.mkString(","),
//            "requiredSolrFieldList" -> x._2.requiredSolrFieldList.mkString(","),
//        ))
//    val arrayStructureSchema =
//      new StructType()
//        .add("dict", MapType(ShortType, MapType(StringType, StringType)))
//    if (logger.isDebugEnabled)
//      logger.debug(s"convertDictTagsToDf with convert_dict=${convert_dict.toString()}")
//    val df_dict =
//      spark.createDataFrame(ListBuffer(List(Row(convert_dict)): _*), arrayStructureSchema)
//    if (logger.isDebugEnabled)
//      logger.debug(s"convertDictTagsToDf output df_dict=$df_dict")
//    df_dict
//  }
}
