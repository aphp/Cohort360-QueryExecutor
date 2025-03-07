package fr.aphp.id.eds.requester.query.resolver.solr

import fr.aphp.id.eds.requester.SolrCollection._
import fr.aphp.id.eds.requester._
import fr.aphp.id.eds.requester.query.model.{BasicResource, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.query.resolver.{ResourceConfig, ResourceResolver}
import fr.aphp.id.eds.requester.tools.SolrTools
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.sql.functions.{array, array_join, col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Class for questioning solr. */
class SolrQueryResolver(solrSparkReader: SolrSparkReader,
                        solrTools: SolrTools = new SolrTools(AppConfig.get.solr.get))
    extends ResourceResolver {
  private val logger = Logger.getLogger(this.getClass)
  private val qbConfigs = new SolrQueryElementsConfig

  def getResourceDataFrame(
      resource: BasicResource,
      criterionTags: CriterionTags,
      sourcePopulation: SourcePopulation)(implicit spark: SparkSession): DataFrame = {
    val solrFilterQuery =
      getSolrFilterQuery(sourcePopulation, resource.resourceType, resource.filter)
    val solrFilterList = getSolrFilterList(criterionTags, resource.patientAge.isDefined)
    val solrCollection = SolrCollections.mapping.getOrElse(
      resource.resourceType,
      throw new Exception(s"Fhir resource ${resource.resourceType} not found in SolR mapping."))
    var criterionDataFrame =
      solrSparkReader.readDf(solrCollection, solrFilterQuery, solrFilterList, resource._id)
    // Group by exploded resources
    val resourceConfig = qbConfigs.requestKeyPerCollectionMap(resource.resourceType)
    criterionDataFrame = if (resourceConfig.contains(QueryColumn.GROUP_BY)) {
      criterionDataFrame
        .drop(resourceConfig(QueryColumn.ID).head)
        .withColumnRenamed(resourceConfig(QueryColumn.GROUP_BY).head,
                           resourceConfig(QueryColumn.ID).head)
        .dropDuplicates(resourceConfig(QueryColumn.ID).head)
    } else {
      criterionDataFrame
    }
    val convFunc = (columnName: String) =>
      qbConfigs.reverseColumnMapping(resource.resourceType, columnName)

    if (resource.uniqueFields.isDefined) {
      val codeColumns = qbConfigs.requestKeyPerCollectionMap(resource.resourceType).getOrElse(QueryColumn.CODE, List())
      if (codeColumns.nonEmpty) {
        criterionDataFrame = criterionDataFrame.withColumn(
          QueryColumn.CODE,
          array_join(array(codeColumns.map((c) => col(s"`${c}`")): _*), ",")
        )
      }
    }
    criterionDataFrame.toDF(criterionDataFrame.columns.map(c => convFunc(c)).toSeq: _*)
  }

  def countPatients(sourcePopulation: SourcePopulation): Long = {
    val solr = solrTools.getSolrClient
    val query =
      new SolrQuery("*:*")
        .addFilterQuery(getDefaultSolrFilterQuery(sourcePopulation, FhirResource.PATIENT))
    val res = solr.query(SolrCollection.PATIENT_APHP, query)
    solr.close()
    res.getResults.getNumFound
  }

  def getDefaultFilterQueryPatient(sourcePopulation: SourcePopulation): String = {
    getDefaultSolrFilterQuery(sourcePopulation, FhirResource.PATIENT) +
      " AND active:true" +
      " AND -(meta.security:\"http://terminology.hl7.org/CodeSystem/v3-ActCode|NOLIST\")"
  }

  private def getDefaultSolrFilterQuery(sourcePopulation: SourcePopulation,
                                        resourceType: String): String = {
    if (!AppConfig.get.business.queryConfig.useSourcePopulation &&
        !(resourceType == FhirResource.PATIENT && AppConfig.get.business.queryConfig.useSourcePopulationOnPatient)) {
      return ""
    }
    val list = sourcePopulation.cohortList.get.map(x => x.toString).mkString(" ")
    s"_list:(${list}) OR ({!join from=resourceId to=_subject fromIndex=groupAphp v='groupId:(${list})' score=none method=crossCollection})"
  }

  /**
    * Determines the field names to ask for solr.
    * */
  private def getSolrFilterList(criterionTags: CriterionTags,
                                isPatientAgeConstraint: Boolean): String = {
    val collectionName: String = criterionTags.resourceType
    val fieldsPerCollectionMap = qbConfigs.requestKeyPerCollectionMap(collectionName)

    val patientAgeColumns =
      if (isPatientAgeConstraint) {
        collectionName match {
          case FhirResource.PATIENT => List(SolrColumn.Patient.BIRTHDATE)
          case _                    => List(SolrColumn.PATIENT_BIRTHDATE)
        }
      } else List()

    val requestedSolrFields = fieldsPerCollectionMap.getOrElse(QueryColumn.PATIENT, List[String]()) ++ criterionTags.requiredFieldList ++ patientAgeColumns

    if (logger.isDebugEnabled) {
      logger.debug(s"requested fields for $collectionName: $requestedSolrFields")
    }

    requestedSolrFields.mkString(",")
  }

  private def getSolrFilterQuery(sourcePopulation: SourcePopulation,
                                 resourceType: String,
                                 filterSolr: String): String = {
    def addDefaultCohortFqParameter(solrFilterQuery: String): String = {
      if (solrFilterQuery == null || solrFilterQuery.isEmpty) {
        return s"fq=${getDefaultSolrFilterQuery(sourcePopulation, resourceType)}"
      }
      s"$solrFilterQuery&fq=${getDefaultSolrFilterQuery(sourcePopulation, resourceType)}"
    }

    addDefaultCohortFqParameter(filterSolr)
  }

  /**
    * Returns the resource configuration for the resource resolver.
    *
    * @return The resource configuration for the resource resolver.
    */
  override def getConfig: ResourceConfig = qbConfigs
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
  val reverseMapping: Map[String, String] = mapping.map(_.swap)
}
