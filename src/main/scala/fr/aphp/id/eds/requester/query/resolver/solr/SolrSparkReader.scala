package fr.aphp.id.eds.requester.query.resolver.solr

import fr.aphp.id.eds.requester.SolrConfig
import fr.aphp.id.eds.requester.tools.SolrTools
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}


trait SolrSparkReader {
  def readDf(solrCollection: String, solrFilterQuery: String, solrFilterList: String, resourceId: Short)(implicit spark: SparkSession): DataFrame
}

class DefaultSolrSparkReader(solrConfig: SolrConfig) extends SolrSparkReader {
  private val logger = Logger.getLogger(this.getClass)
  private val solrConf = new SolrTools(solrConfig).getSolrConf

  // Returning T, throwing the exception on failure
  @annotation.tailrec
  private def retry[T](n: Int)(fn: => T): T = {
    util.Try {
      fn
    } match {
      case util.Success(x) => x
      case _ if n > 1      => retry(n - 1)(fn)
      case util.Failure(e) =>
        throw e
    }
  }

  def readDf(solrCollection: String, solrFilterQuery: String, solrFilterList: String, resourceId: Short)(implicit spark: SparkSession): DataFrame = {
    logger.info(
      s"SolR REQUEST: ${Map("collection" -> solrCollection, "fields" -> solrFilterList, "solr.params" -> solrFilterQuery)}")
    val mapRequest = solrConf.filter(c => c._1 != "max_try") ++ Map(
      "collection" -> solrCollection,
      "fields" -> solrFilterList,
      "solr.params" -> solrFilterQuery)

    import com.lucidworks.spark.util.SolrDataFrameImplicits._
    val df: DataFrame = retry(solrConf.getOrElse("max_try", "1").toInt) {
      spark.read.solr(solrCollection, mapRequest)
    }
    if (logger.isDebugEnabled) {
      logger.debug(
        s"SolR REQUEST : $mapRequest => df.count=${df.count}," +
          s" df.columns=${df.columns.mkString("Array(", ", ", ")")}")
    }
    df
  }

}
