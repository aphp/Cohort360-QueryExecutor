package fr.aphp.id.eds.requester.query.resolver

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Class for questioning solr. */
object SolrQueryResolver extends FhirResourceResolver {
  private val logger = Logger.getLogger(this.getClass)

  // Returning T, throwing the exception on failure
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    util.Try {
      fn
    } match {
      case util.Success(x) => x
      case _ if n > 1      => retry(n - 1)(fn)
      case util.Failure(e) =>
        throw e
    }
  }

  def getSolrResponseDataFrame(resourceType: String,
                               requestedFields: String,
                               requestFilter: String)(implicit spark: SparkSession,
                                                      solrConf: Map[String, String],
                                                      resourceId: Short): DataFrame = {
    import com.lucidworks.spark.util.SolrDataFrameImplicits._
    logger.info(
      s"SolR REQUEST: ${Map("collection" -> resourceType, "fields" -> requestedFields, "solr.params" -> requestFilter)}")

    val mapRequest = solrConf.filter(c => c._1 != "max_try") ++ Map("collection" -> resourceType,
                                                                    "fields" -> requestedFields,
                                                                    "solr.params" -> requestFilter)
    val df: DataFrame = retry(solrConf.getOrElse("max_try", "1").toInt) {
      spark.read.solr(resourceType, mapRequest)
    }
    if (logger.isDebugEnabled) {
      logger.debug(
        s"SolR REQUEST : $mapRequest => df.count=${df.count}," +
          s" df.columns=${df.columns.mkString("Array(", ", ", ")")}")
    }
    df
  }
}