package fr.aphp.id.eds.requester.query.resolver.rest

import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor
import fr.aphp.id.eds.requester.FhirServerConfig
import org.hl7.fhir.r4.model.Bundle

trait RestFhirClient {
  def getFhirContext: FhirContext

  def getBundle(resourceType: String, filter: String, elements: List[String]): Bundle

  def getNextPage(bundle: Bundle): Bundle

  def getClient: IGenericClient
}

class DefaultRestFhirClient(fhirConfig: FhirServerConfig,
                            cohortServer: Boolean = false,
                            options: Map[String, String] = Map.empty)
    extends RestFhirClient {
  private val ctx = FhirContext.forR4()
  private val client = ctx.newRestfulGenericClient(getServerUrl)
  private val authInterceptor =
    if (options.contains("accessToken")) {
      Some(new BearerTokenAuthInterceptor(options("accessToken")))
    } else {
      fhirConfig.accessToken.flatMap(token => Some(new BearerTokenAuthInterceptor(token)))
    }
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

  override def getClient: IGenericClient = client

  private def getServerUrl: String = {
    if (cohortServer) {
      fhirConfig.cohortUrl.getOrElse(fhirConfig.url)
    } else {
      fhirConfig.url
    }
  }
}
