package fr.aphp.id.eds.requester.query.resolver.rest

import ca.uhn.fhir.fhirpath.IFhirPath
import ca.uhn.fhir.util.BundleUtil
import fr.aphp.id.eds.requester.FhirResource
import fr.aphp.id.eds.requester.query.model.{BasicResource, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.query.resolver.ResourceResolver
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.hl7.fhir.instance.model.api.IBase
import org.hl7.fhir.r4.model.Bundle

import scala.collection.JavaConverters._

class RestFhirResolver(fhirClient: RestFhirClient) extends ResourceResolver {
  private val ctx = fhirClient.getFhirContext
  private val batchSize = 1000
  private val qbConfigs = new RestFhirQueryElementsConfig

  override def getResourceDataFrame(
      resource: BasicResource,
      criterionTags: CriterionTags,
      sourcePopulation: SourcePopulation)(implicit spark: SparkSession): DataFrame = {
    val results: Bundle = fhirClient.getBundle(
      resource.resourceType,
      resource.filter,
      getSubsetElementsFilter(criterionTags.requiredFieldList))
    val mapping = qbConfigs
      .fhirPathMappings(resource.resourceType)
      .filter(el => criterionTags.requiredFieldList.contains(el.columnMapping.fhirPath))
    val resourceInnerMapping = mapping.filter(_.joinInfo.isEmpty).map(_.columnMapping)
    var resultsDf = createDataFrameFromBundle(results, resourceInnerMapping)
    var nextResults = getNextBatch(results, batchSize)
    while (results != null) {
      resultsDf = resultsDf.union(createDataFrameFromBundle(nextResults, resourceInnerMapping))
      nextResults = getNextBatch(nextResults, batchSize)
    }

    val joinResourceInfo = getJoinResourceInfo(mapping)
    joinResourceInfo.foreach((rq) => {
      resultsDf = joinResource(resultsDf, rq._1.resource, rq._1.sourceJoinColumn, rq._2)
    })
    resultsDf
  }

  override def countPatients(sourcePopulation: SourcePopulation): Long = {
    fhirClient
      .getBundle(FhirResource.PATIENT,
                 getDefaultFilterQueryPatient(sourcePopulation) + "&_count=0",
                 List())
      .getTotal
  }

  def getDefaultFilterQueryPatient(sourcePopulation: SourcePopulation): String = {
    val list = sourcePopulation.caresiteCohortList.get.map(x => x.toString).mkString(" ")
    val securityNoList =
      java.net.URLEncoder.encode("http://terminology.hl7.org/CodeSystem/v3-ActCode|NOLIST", "UTF-8")
    s"_list=$list&active=true&meta.security:not=$securityNoList"
  }

  def getJoinResourceInfo(
      requestedColumns: List[ResourceMapping]): Map[JoinInfo, List[QueryColumnMapping]] = {
    requestedColumns
      .filter(_.joinInfo.isDefined)
      .map(rm => rm.joinInfo.get -> rm.columnMapping)
      .groupBy(_._1)
      .mapValues(_.map(_._2))
  }

  def joinResource(
      resultDf: DataFrame,
      resourceType: String,
      joinColumn: String,
      resourceQueryColumns: List[QueryColumnMapping])(implicit spark: SparkSession): DataFrame = {
    val resourceIds =
      resultDf.select(joinColumn).dropDuplicates().collect().map(row => row.getString(0))
    val resourceIdsChunks = resourceIds.grouped(1000).toList
    val resourceIdsChunksRDD = spark.sparkContext.parallelize(resourceIdsChunks)
    val dfToJoin = resourceIdsChunksRDD
      .map(chunk => {
        val results: Bundle =
          fhirClient.getBundle(resourceType,
                               f"_id=${chunk.mkString(",")}",
                               getSubsetElementsFilter(resourceQueryColumns.map(_.fhirPath)))
        createDataFrameFromBundle(results, resourceQueryColumns)
      })
      .reduce((df1, df2) => df1.union(df2))
    resultDf.join(dfToJoin, resultDf(joinColumn) === dfToJoin("id"))

  }

  def getNextBatch(bundle: Bundle, offset: Int): Bundle = {
    val nextLink = bundle.getLink("next")
    if (nextLink != null) {
      if (!bundle.hasTotal || bundle.getTotal > offset + batchSize) {
        fhirClient.getNextPage(bundle)
      } else {
        null
      }
    } else {
      null
    }
  }

  private def getSubsetElementsFilter(filterColList: List[String]): List[String] = {
    filterColList.map(col => col.split(".").head).distinct
  }

  def createDataFrameFromBundle(bundle: Bundle, mapping: List[QueryColumnMapping])(
      implicit spark: SparkSession): DataFrame = {
    val fhirPathEvaluator = ctx.newFhirPath()
    val rows = spark.sparkContext
      .parallelize(BundleUtil.toListOfResources(ctx, bundle).asScala)
      .map(resource => createRowFromResource(resource, mapping, fhirPathEvaluator))
    val schema = StructType(
      mapping.map(
        el =>
          StructField(el.queryColName,
                      translateFhirTypesToSparkTypes(el.fhirType),
                      nullable = el.nullable)))
    spark.createDataFrame(rows, schema)
  }

  def createRowFromResource(resource: IBase,
                            mapping: List[QueryColumnMapping],
                            fhirPathEngine: IFhirPath): Row = {
    Row.fromSeq(mapping.map(el => extractElementFromResource(resource, el, fhirPathEngine)))
  }

  def extractElementFromResource(resource: IBase,
                                 mapping: QueryColumnMapping,
                                 fhirPathEngine: IFhirPath): Any = {
    mapFhirTypesToSparkTypes(fhirPathEngine.evaluate(resource, mapping.fhirPath, mapping.fhirType))
  }

  def translateFhirTypesToSparkTypes(fhirType: Class[_]): DataType = {
    fhirType match {
      case _: Class[org.hl7.fhir.r4.model.StringType]   => StringType
      case _: Class[org.hl7.fhir.r4.model.DateType]     => DateType
      case _: Class[org.hl7.fhir.r4.model.DateTimeType] => DateType
      case _: Class[org.hl7.fhir.r4.model.Reference]    => StringType
      case _                                            => StringType
    }
  }

  def mapFhirTypesToSparkTypes(value: Any): Any = {
    value match {
      case _: org.hl7.fhir.r4.model.StringType =>
        value.asInstanceOf[org.hl7.fhir.r4.model.StringType].getValue
      case _: org.hl7.fhir.r4.model.DateType =>
        value.asInstanceOf[org.hl7.fhir.r4.model.DateType].getValue
      case _: org.hl7.fhir.r4.model.DateTimeType =>
        value.asInstanceOf[org.hl7.fhir.r4.model.DateTimeType].getValue
      case _: org.hl7.fhir.r4.model.Reference =>
        value.asInstanceOf[org.hl7.fhir.r4.model.Reference].getReferenceElement.getIdPart
      case _ => value.toString
    }
  }
}
