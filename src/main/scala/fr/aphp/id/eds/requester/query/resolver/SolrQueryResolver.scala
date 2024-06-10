package fr.aphp.id.eds.requester.query.resolver

import fr.aphp.id.eds.requester.SolrCollection._
import fr.aphp.id.eds.requester.tools.SolrTools
import fr.aphp.id.eds.requester.{FhirResource, SolrConfig}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Class for questioning solr. */
class SolrQueryResolver(solrConfig: SolrConfig) extends FhirResourceResolver {
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

  def getSolrResponseDataFrame(
      resourceType: String,
      requestedFields: String,
      requestFilter: String)(implicit spark: SparkSession, resourceId: Short): DataFrame = {
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

object SolrCollections {
  val mapping: Map[String, String] = Map(
    FhirResource.PATIENT -> PATIENT_APHP,
    FhirResource.ENCOUNTER -> ENCOUNTER_APHP,
    FhirResource.OBSERVATION -> OBSERVATION_APHP,
    FhirResource.CONDITION -> CONDITION_APHP,
    FhirResource.PROCEDURE -> PROCEDURE_APHP,
    FhirResource.DOCUMENT_REFERENCE -> DOCUMENTREFERENCE_APHP,
    FhirResource.CLAIM -> CLAIM_APHP,
    FhirResource.COMPOSITION -> COMPOSITION_APHP,
    FhirResource.GROUP -> GROUP_APHP,
    FhirResource.MEDICATION_REQUEST -> MEDICATIONREQUEST_APHP,
    FhirResource.MEDICATION_ADMINISTRATION -> MEDICATIONADMINISTRATION_APHP,
    FhirResource.IMAGING_STUDY -> IMAGINGSTUDY_APHP,
    FhirResource.QUESTIONNAIRE_RESPONSE -> QUESTIONNAIRE_RESPONSE_APHP,
  )
}
