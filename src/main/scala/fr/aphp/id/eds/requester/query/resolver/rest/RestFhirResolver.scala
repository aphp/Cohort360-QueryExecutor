package fr.aphp.id.eds.requester.query.resolver.rest

import ca.uhn.fhir.fhirpath.IFhirPath
import ca.uhn.fhir.util.BundleUtil
import fr.aphp.id.eds.requester.query.model.{BasicResource, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.query.resolver.{ResourceConfig, ResourceResolver}
import fr.aphp.id.eds.requester.{AppConfig, FhirResource, QueryColumn}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.hl7.fhir.instance.model.api.IBase
import org.hl7.fhir.r4.model.{Bundle, IdType, Reference}

import java.util.Optional
import scala.collection.JavaConverters._

/**
  * Implementation of ResourceResolver that uses a RestFhirClient to fetch resources from a FHIR server.
  * @param fhirClient The RestFhirClient to use for fetching resources.
  */
class RestFhirResolver(fhirClient: RestFhirClient) extends ResourceResolver {
  private val logger = Logger.getLogger(this.getClass)
  private val ctx = fhirClient.getFhirContext
  private val batchSize = 1000
  private val qbConfigs = new RestFhirQueryElementsConfig

  override def getResourceDataFrame(
      resource: BasicResource,
      criterionTags: CriterionTags,
      sourcePopulation: SourcePopulation)(implicit spark: SparkSession): DataFrame = {
    // add the patient field (id) to the required field list
    val requiredFieldList = addPatientRequiredField(resource, criterionTags.requiredFieldList)
    // retrieve the first page of the resource
    logger.info(s"Fetching ${resource.resourceType} resources with filter ${resource.filter} and required fields $requiredFieldList")
    val results: Bundle = fhirClient.getBundle(
      resource.resourceType,
      addSourcePopulationConstraint(sourcePopulation, resource.filter),
      getSubsetElementsFilter(requiredFieldList))
    // retrieve the query columns mapping of the resource (only those requested by the requiredFieldList)
    val mapping = qbConfigs
      .fhirPathMappings(resource.resourceType)
      .filter(el => requiredFieldList.contains(el.columnMapping.fhirPath))
    // restrict the column mapping to only the one that are inner to the resource (not those needing a joined resource)
    val resourceInnerMapping = mapping
      .map(
        colMap => {
          if (colMap.joinInfo.isDefined) {
            QueryColumnMapping(
              colMap.joinInfo.get.sourceJoinColumn,
              colMap.joinInfo.get.sourceJoinColumn,
              classOf[Reference],
              colMap.columnMapping.nullable
            )
          } else { colMap.columnMapping }
        }
      )
      .groupBy(_.queryColName)
      .map(_._2.head)
      .toList
    // retrieve all pages of the resource
    var resultsDf = getAllPagesOfResource(results, resourceInnerMapping)
    // get the list of other resource to join for the requested columns that need it
    val joinResourceInfo = getJoinResourceInfo(mapping)
    // join the other resources with the remaining columns
    joinResourceInfo.foreach((rq) => {
      resultsDf =
        joinResource(resultsDf, sourcePopulation, rq._1.resource, rq._1.sourceJoinColumn, rq._2)
    })
    // select only the requested columns with their generic alias query column name
    val selectedColumns = mapping.map(m => m.columnMapping.queryColName)
    logger.info(s"Returning the DataFrame with the selected columns $selectedColumns")
    resultsDf.select(selectedColumns.map(col): _*)
  }

  override def countPatients(sourcePopulation: SourcePopulation): Long = {
    fhirClient
      .getBundle(FhirResource.PATIENT,
                 getDefaultFilterQueryPatient(sourcePopulation) + "&_count=0",
                 List())
      .getTotal
  }

  override def getDefaultFilterQueryPatient(sourcePopulation: SourcePopulation): String = {
    val securityNoList =
      java.net.URLEncoder.encode("http://terminology.hl7.org/CodeSystem/v3-ActCode|NOLIST", "UTF-8")
    addSourcePopulationConstraint(sourcePopulation,
                                  s"active=true&_security:not=$securityNoList")
  }

  private def addPatientRequiredField(resource: BasicResource,
                                      requiredFieldList: List[String]): List[String] = {
    (requiredFieldList ++ qbConfigs
      .requestKeyPerCollectionMap(resource.resourceType)
      .getOrElse(QueryColumn.PATIENT, List[String]())).distinct
  }

  private def getAllPagesOfResource(results: Bundle, mapping: List[QueryColumnMapping])(
      implicit spark: SparkSession) = {
    logger.debug("Fetching all pages of the resource...")
    var resultsDf = createDataFrameFromBundle(results, mapping)
    var nextResults = getNextBatch(results, batchSize)
    while (nextResults != null) {
      resultsDf = resultsDf.union(createDataFrameFromBundle(nextResults, mapping))
      nextResults = getNextBatch(nextResults, batchSize)
    }
    resultsDf
  }

  private def addSourcePopulationConstraint(sourcePopulation: SourcePopulation,
                                            filter: String): String = {
    if (!AppConfig.get.business.queryConfig.useSourcePopulation) {
      return filter
    }
    val list = sourcePopulation.caresiteCohortList.get.map(x => x.toString).mkString(",")
    val constraint = s"_list=$list"
    if (filter.trim.isEmpty) {
      constraint
    } else {
      s"$filter&$constraint"
    }
  }

  private def getJoinResourceInfo(
      requestedColumns: List[ResourceMapping]): Map[JoinInfo, List[QueryColumnMapping]] = {
    requestedColumns
      .filter(_.joinInfo.isDefined)
      .map(rm => rm.joinInfo.get -> rm.columnMapping)
      .groupBy(_._1)
      .mapValues(_.map(m =>
        m._2.copy(fhirPath = m._2.fhirPath.stripPrefix(m._1.sourceJoinColumn + "."))) ++ List(
        QueryColumnMapping("id", "id", classOf[IdType], nullable = false)))
  }

  private def joinResource(
      resultDf: DataFrame,
      sourcePopulation: SourcePopulation,
      resourceType: String,
      joinColumn: String,
      resourceQueryColumns: List[QueryColumnMapping])(implicit spark: SparkSession): DataFrame = {
    val resourceIds =
      resultDf.select(joinColumn).dropDuplicates().collect().map(row => row.getString(0))
    val resourceIdsChunks = resourceIds.filter(_ != null).grouped(1000).toList
    val dfToJoin = resourceIdsChunks
      .map(chunk => {
        logger.info(s"Fetching join resource $resourceType")
        val results: Bundle =
          fhirClient.getBundle(
            resourceType,
            addSourcePopulationConstraint(sourcePopulation, f"_id=${chunk.sorted.mkString(",")}"),
            getSubsetElementsFilter(resourceQueryColumns.map(c => c.fhirPath))
          )
        getAllPagesOfResource(results, resourceQueryColumns)
      })
      .reduce((df1, df2) => df1.union(df2))
    logger.info("Joining resources...")
    resultDf
      .alias("origin")
      .join(dfToJoin.alias("j"), col(s"origin.${joinColumn}") === col("j.id"), "left")
      .select(
        List(col("origin.*")) ++ resourceQueryColumns
          .filter(_.queryColName != "id")
          .map(c => col(s"j.`${c.queryColName}`")): _*)
  }

  private def getNextBatch(bundle: Bundle, offset: Int): Bundle = {
    val nextLink = bundle.getLink("next")
    if (nextLink != null) {
      if (!bundle.hasTotal || bundle.getTotal > offset + batchSize) {
        logger.debug("Fetching next page of the resource: " + nextLink.getUrl)
        fhirClient.getNextPage(bundle)
      } else {
        null
      }
    } else {
      null
    }
  }

  private def getSubsetElementsFilter(filterColList: List[String]): List[String] = {
    filterColList.map(col => col.split("\\.").head).distinct
  }

  private def createDataFrameFromBundle(bundle: Bundle, mapping: List[QueryColumnMapping])(
      implicit spark: SparkSession): DataFrame = {
    val fhirPathEvaluator = ctx.newFhirPath()
    val rows = spark.sparkContext
      .parallelize(
        BundleUtil
          .toListOfResources(ctx, bundle)
          .asScala
          .map(resource => createRowFromResource(resource, mapping, fhirPathEvaluator)))

    val schema = StructType(
      mapping.map(
        el =>
          StructField(el.queryColName,
                      translateFhirTypesToSparkTypes(el.fhirType),
                      nullable = el.nullable)))
    spark.createDataFrame(rows, schema)
  }

  private def createRowFromResource(resource: IBase,
                                    mapping: List[QueryColumnMapping],
                                    fhirPathEngine: IFhirPath): Row = {
    Row.fromSeq(mapping.map(el => extractElementFromResource(resource, el, fhirPathEngine)))
  }

  private def extractElementFromResource(resource: IBase,
                                         mapping: QueryColumnMapping,
                                         fhirPathEngine: IFhirPath): Any = {
    mapFhirTypesToSparkTypes(
      fhirPathEngine.evaluateFirst(resource, mapping.fhirPath, mapping.fhirType))
  }

  private def translateFhirTypesToSparkTypes(fhirType: Class[_]): DataType = {
    fhirType.getTypeName match {
      case "org.hl7.fhir.r4.model.IdType"       => StringType
      case "org.hl7.fhir.r4.model.StringType"   => StringType
      case "org.hl7.fhir.r4.model.DateType"     => DateType
      case "org.hl7.fhir.r4.model.DateTimeType" => TimestampType
      case "org.hl7.fhir.r4.model.Reference"    => StringType
      case _                                    => StringType
    }
  }

  private def mapFhirTypesToSparkTypes(optionalValue: Optional[_ <: IBase]): Any = {
    if (optionalValue.isEmpty) {
      return null
    }
    val value = optionalValue.get()
    value match {
      case _: org.hl7.fhir.r4.model.IdType =>
        value.asInstanceOf[org.hl7.fhir.r4.model.IdType].getIdPart
      case _: org.hl7.fhir.r4.model.StringType =>
        value.asInstanceOf[org.hl7.fhir.r4.model.StringType].getValue
      case _: org.hl7.fhir.r4.model.DateType =>
        java.sql.Date.valueOf(
          value
            .asInstanceOf[org.hl7.fhir.r4.model.DateType]
            .toHumanDisplayLocalTimezone
        )
      case _: org.hl7.fhir.r4.model.DateTimeType =>
        java.sql.Timestamp.from(
          value
            .asInstanceOf[org.hl7.fhir.r4.model.DateTimeType]
            .getValue
            .toInstant
        )
      case _: org.hl7.fhir.r4.model.Reference =>
        value.asInstanceOf[org.hl7.fhir.r4.model.Reference].getReferenceElement.getIdPart
      case _ => value.toString
    }
  }

  /**
    * Returns the resource configuration for the resource resolver.
    *
    * @return The resource configuration for the resource resolver.
    */
  override def getConfig: ResourceConfig = qbConfigs
}
