package fr.aphp.id.eds.requester.query.resolver

import fr.aphp.id.eds.requester.AppConfig
import fr.aphp.id.eds.requester.query.resolver.rest.{RestFhirQueryElementsConfig, RestFhirResolver}
import fr.aphp.id.eds.requester.query.resolver.solr.{SolrQueryElementsConfig, SolrQueryResolver}

object FhirResourceResolverFactory {
  def getDefault: FhirResourceResolver = {
    AppConfig.get.defaultResolver match {
      case "solr" => new SolrQueryResolver(AppConfig.get.solr.get)
      case "fhir" => new RestFhirResolver(AppConfig.get.fhir.get)
      case _ => throw new IllegalArgumentException("No default resolver found")
    }
  }

  def getDefaultConfig: QueryElementsConfig = {
    AppConfig.get.defaultResolver match {
      case "solr" => new SolrQueryElementsConfig
      case "fhir" => new RestFhirQueryElementsConfig
      case _ => throw new IllegalArgumentException("No default config found")
    }
  }
}
