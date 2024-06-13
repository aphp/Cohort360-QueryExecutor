package fr.aphp.id.eds.requester.query.resolver

import fr.aphp.id.eds.requester.SolrCollection._
import fr.aphp.id.eds.requester.query.engine.QueryBuilderConfigs
import fr.aphp.id.eds.requester.query.model.{BasicResource, Request, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.tools.JobUtils.getDefaultSolrFilterQuery
import fr.aphp.id.eds.requester.tools.SolrTools
import fr.aphp.id.eds.requester.{AppConfig, FhirResource, PATIENT_COL, SolrCollection, SolrColumn, SolrConfig}
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Class for questioning solr. */
class SolrQueryResolver(solrConfig: SolrConfig) extends FhirResourceResolver {
  private val logger = Logger.getLogger(this.getClass)
  private val solrConf = new SolrTools(solrConfig).getSolrConf
  private val qbConfigs = new QueryBuilderConfigs()

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
                                resource: BasicResource,
                                criterionTags: CriterionTags,
                                sourcePopulation: SourcePopulation)(implicit spark: SparkSession): DataFrame = {
    val solrFilterQuery = getSolrFilterQuery(sourcePopulation, resource)
    val solrFilterList = getSolrFilterList(criterionTags, resource.patientAge.isDefined)
    logger.info(
      s"SolR REQUEST: ${Map("collection" -> resource.resourceType, "fields" -> solrFilterList, "solr.params" -> solrFilterQuery)}")

    val mapRequest = solrConf.filter(c => c._1 != "max_try") ++ Map("collection" -> resource.resourceType,
                                                                    "fields" -> solrFilterList,
                                                                    "solr.params" -> solrFilterQuery)
    import com.lucidworks.spark.util.SolrDataFrameImplicits._
    val df: DataFrame = retry(solrConf.getOrElse("max_try", "1").toInt) {
      spark.read.solr(resource.resourceType, mapRequest)
    }
    if (logger.isDebugEnabled) {
      logger.debug(
        s"SolR REQUEST : $mapRequest => df.count=${df.count}," +
          s" df.columns=${df.columns.mkString("Array(", ", ", ")")}")
    }
    df
  }

  def countPatients(sourcePopulation: SourcePopulation): Long = {
    val solr = new SolrTools(AppConfig.get.solr.get).getSolrClient
    val query =
      new SolrQuery("*:*").addFilterQuery(getDefaultSolrFilterQuery(sourcePopulation))
    val res = solr.query(SolrCollection.PATIENT_APHP, query)
    solr.close()
    res.getResults.getNumFound
  }

  /**
   * Determines the field names to ask for solr.
   * */
  private def getSolrFilterList(criterionTags: CriterionTags, isPatientAgeConstraint: Boolean): String = {
    val collectionName: String = criterionTags.resourceType
    val fieldsPerCollectionMap = qbConfigs.requestKeyPerCollectionMap(collectionName)

    def addRequiredFields(requestedAttributes: List[String]): List[String] = {
      requestedAttributes ++ criterionTags.requiredSolrFieldList
    }

    def addPatientAgeRequiredAttributes(requestedAttributes: List[String]): List[String] = {
      if (isPatientAgeConstraint) {
        collectionName match {
          case SolrCollection.PATIENT_APHP => SolrColumn.Patient.BIRTHDATE :: requestedAttributes
          case _                           => SolrColumn.PATIENT_BIRTHDATE :: requestedAttributes
        }
      } else requestedAttributes
    }

    val requestedSolrFields = addPatientAgeRequiredAttributes(
      addRequiredFields(
        fieldsPerCollectionMap.getOrElse(PATIENT_COL, List[String]())
      )
    )
    if (logger.isDebugEnabled) {
      logger.debug(s"requested fields for $collectionName: $requestedSolrFields")
    }

    requestedSolrFields.mkString(",")
  }

  private def getSolrFilterQuery(sourcePopulation: SourcePopulation,
                                           basicResource: BasicResource): String = {
    def addDefaultCohortFqParameter(solrFilterQuery: String): String = {
      if (solrFilterQuery == null || solrFilterQuery.isEmpty) {
        return s"fq=${getDefaultSolrFilterQuery(sourcePopulation)}"
      }
      s"$solrFilterQuery&fq=${getDefaultSolrFilterQuery(sourcePopulation)}"
    }

    addDefaultCohortFqParameter(basicResource.filter)
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
