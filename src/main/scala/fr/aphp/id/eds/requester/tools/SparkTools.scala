package fr.aphp.id.eds.requester.tools

import org.apache.spark.sql.functions.{col, hash}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object SparkTools {



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
  
}
