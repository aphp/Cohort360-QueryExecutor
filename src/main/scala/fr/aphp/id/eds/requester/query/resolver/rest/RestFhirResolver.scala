package fr.aphp.id.eds.requester.query.resolver.rest

import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.util.BundleUtil
import fr.aphp.id.eds.requester.FhirServerConfig
import fr.aphp.id.eds.requester.query.model.{BasicResource, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.query.resolver.FhirResourceResolver
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.hl7.fhir.r4.model.{Bundle, Patient}

import scala.collection.convert.ImplicitConversions.`collection asJava`

class RestFhirResolver(fhirConf: FhirServerConfig) extends FhirResourceResolver {

  override def getSolrResponseDataFrame(resource: BasicResource,
                                        criterionTags: CriterionTags,
                                        sourcePopulation: SourcePopulation)(implicit spark: SparkSession): DataFrame = {
    val ctx = FhirContext.forR4()
    val client = ctx.newRestfulGenericClient(fhirConf.url)

    // Perform a search
    val resources: List[Patient] = List()
    val results: Bundle = client
      .search
      .byUrl(f"${resource.resourceType}?${resource.filter}")
      .elementsSubset(criterionTags.requiredSolrFieldList:_*)
      .returnBundle(classOf[Bundle])
      .execute

    resources.addAll(BundleUtil.toListOfResources(ctx, results).asInstanceOf[java.util.List[Patient]])


    val schema = StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("gender", StringType, nullable = true)
    ))

    def patientToRow(patient: Patient): Row = {
      val id = if (patient.hasId) patient.getIdElement.getIdPart else null
      val name = if (patient.hasName) patient.getName.get(0).getNameAsSingleString else null
      val gender = if (patient.hasGender) patient.getGender.toCode else null

      Row(id, name, gender)
    }

    val rows: List[Row] = resources.map(patientToRow)
    val rdd = spark.sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, schema)
    df
  }

  override def countPatients(sourcePopulation: SourcePopulation): Long = {
    val ctx = FhirContext.forR4()
    val client = ctx.newRestfulGenericClient(fhirConf.url)
    val results: Bundle = client
      .search
      .forResource(classOf[Patient])
      .count(0)
      .returnBundle(classOf[Bundle])
      .execute
    results.getTotal
  }
}
