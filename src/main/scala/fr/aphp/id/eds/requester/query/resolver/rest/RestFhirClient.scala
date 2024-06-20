package fr.aphp.id.eds.requester.query.resolver.rest

import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor
import fr.aphp.id.eds.requester.FhirServerConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.hl7.fhir.r4.model.Bundle

trait RestFhirClient {
  def getFhirContext: FhirContext

  def getBundle(resourceType: String, filter: String, elements: List[String]): Bundle

  def getNextPage(bundle: Bundle): Bundle
}

class DefaultRestFhirClient(fhirConfig: FhirServerConfig) extends RestFhirClient {
  private val ctx = FhirContext.forR4()
  private val client = ctx.newRestfulGenericClient(fhirConfig.url)
  private val authInterceptor = fhirConfig.accessToken.flatMap(token => Some(new BearerTokenAuthInterceptor(token)))
  authInterceptor.foreach(client.registerInterceptor)

  override def getFhirContext: FhirContext = ctx

  override def getBundle(resourceType: String, filter: String, elements: List[String]): Bundle = {
    client.search
      .byUrl(f"${resourceType}?${filter}")
      .elementsSubset(elements: _*)
      .returnBundle(classOf[Bundle])
      .execute
  }

  override def getNextPage(bundle: Bundle): Bundle = {
    client
      .loadPage()
      .next(bundle)
      .execute
  }
}
