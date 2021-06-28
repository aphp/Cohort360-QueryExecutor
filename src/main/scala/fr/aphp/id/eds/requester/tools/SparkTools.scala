package fr.aphp.id.eds.requester.tools

import org.apache.spark.sql.functions.{col, hash}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object SparkTools {

  /**
    * Adds a hash column based on several other columns
    *
    * @param df               DataFrame
    * @param columnsToExclude List[String] the columns not to be hashed
    * @return DataFrame
    */
  def dfAddHash(
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

  /**
    * Récupère un dataframe du cache s'il existe
    *
    * @param spark  current SparkSession
    * @param hash   hash of the dataframe to recover
    * @return
    */
  def getCached(
      spark: SparkSession,
      hash: String,
      user: String
  ): Option[DataFrame] =
    spark.catalog.listTables
      .filter("isTemporary")
      .filter(s"lower(name) = lower('${hash}_$user')")
      .count match {
      case 1L =>
        val resultDf = Some(
          spark.sql(s"select * from `${hash.toLowerCase()}_$user`"))
        resultDf
      case _ => None
    }

  /**
    * met en cache un dataframe
    *
    * @param hash   hash of the dataframe to recover
    * @param df     dataframe to cache
    */
  def putCached(hash: String, user: String, df: DataFrame): Unit = {
    df.persist(StorageLevel.DISK_ONLY)
      .createOrReplaceTempView(s"${hash.toLowerCase()}_$user")
  }

  /** purge un dataframe du cache
    *
    *  */
  def purgeCached(spark: SparkSession,
                  hash: Option[String],
                  user: Option[String]): Any = {
    if (hash.isDefined & user.isDefined) {
      spark
        .sql(s"select * from `${hash.get.toLowerCase()}_${user.get}`")
        .unpersist
    } else {
      var listTables =
        spark.catalog.listTables.toDF().collect().map(_.getString(0)).toList
      listTables =
        if (user.isDefined)
          listTables.filter(x => x.contains(user.get.toString))
        else listTables
      for (table <- listTables) {
        spark.sql(s"select * from `$table`").unpersist
        spark.sql(s"DROP TABLE IF EXISTS `$table`")
      }
    }
  }
}
