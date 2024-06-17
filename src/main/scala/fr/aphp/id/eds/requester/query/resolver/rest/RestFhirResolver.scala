package fr.aphp.id.eds.requester.query.resolver.rest

import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.fhirpath.IFhirPath
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.util.BundleUtil
import fr.aphp.id.eds.requester.FhirServerConfig
import fr.aphp.id.eds.requester.query.model.{BasicResource, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.query.resolver.ResourceResolver
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.hl7.fhir.instance.model.api.IBase
import org.hl7.fhir.r4.model.{Bundle, Patient, Quantity}

import scala.collection.JavaConverters._

class RestFhirResolver(fhirConf: FhirServerConfig) extends ResourceResolver {
  private val ctx = FhirContext.forR4()
  private val batchSize = 1000
  private val qbConfigs = new RestFhirQueryElementsConfig

  override def getResourceDataFrame(
      resource: BasicResource,
      criterionTags: CriterionTags,
      sourcePopulation: SourcePopulation)(implicit spark: SparkSession): DataFrame = {
    val client = ctx.newRestfulGenericClient(fhirConf.url)
    val results: Bundle = client.search
      .byUrl(f"${resource.resourceType}?${resource.filter}")
      .elementsSubset(criterionTags.requiredFieldList: _*)
      .returnBundle(classOf[Bundle])
      .execute

    val mapping = qbConfigs
      .fhirPathMappings(resource.resourceType)
      .filter(el => criterionTags.requiredFieldList.contains(el.fhirPath))
    var resultsDf = createDataFrameFromBundle(results, mapping)
    var nextResults = getNextBatch(results, client, batchSize)
    while (results != null) {
      resultsDf = resultsDf.union(createDataFrameFromBundle(nextResults, mapping))
      nextResults = getNextBatch(results, client, batchSize)
    }
    resultsDf
  }

  override def countPatients(sourcePopulation: SourcePopulation): Long = {
    val ctx = FhirContext.forR4()
    val client = ctx.newRestfulGenericClient(fhirConf.url)
    val results: Bundle = client.search
      .forResource(classOf[Patient])
      .count(0)
      .returnBundle(classOf[Bundle])
      .execute
    results.getTotal
  }

  def getNextBatch(bundle: Bundle, client: IGenericClient, offset: Int): Bundle = {
    val nextLink = bundle.getLink("next")
    if (nextLink != null) {
      if (!bundle.hasTotal || bundle.getTotal > offset + batchSize) {
        client
          .loadPage()
          .next(bundle)
          .execute
      } else {
        null
      }
    } else {
      null
    }
  }

  def createDataFrameFromBundle(bundle: Bundle, mapping: List[ResourceMapping])(
      implicit spark: SparkSession): DataFrame = {
    val fhirPathEvaluator = ctx.newFhirPath()
    val rows = spark.sparkContext
      .parallelize(BundleUtil.toListOfResources(ctx, bundle).asScala)
      .map(resource => createRowFromResource(resource, mapping, fhirPathEvaluator))
    val schema = StructType(mapping.map(el =>
      StructField(el.colName, translateFhirTypesToSparkTypes(el.fhirType), nullable = el.nullable)))
    spark.createDataFrame(rows, schema)
  }

  def createRowFromResource(resource: IBase,
                            mapping: List[ResourceMapping],
                            fhirPathEngine: IFhirPath): Row = {
    Row.fromSeq(mapping.map(el => extractElementFromResource(resource, el, fhirPathEngine)))
  }

  def extractElementFromResource(resource: IBase,
                                             mapping: ResourceMapping,
                                             fhirPathEngine: IFhirPath): Any = {
    fhirPathEngine.evaluate(resource, mapping.fhirPath, mapping.fhirType.asSubclass(classOf[IBase]))
  }

  def translateFhirTypesToSparkTypes(fhirType: Class[_]): DataType = {
    fhirType match {
      case _: Class[org.hl7.fhir.r4.model.StringType] => StringType
      case _: Class[Quantity] => StringType
      case _ => StringType
    }
  }
}
