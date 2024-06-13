package fr.aphp.id.eds.requester.query.resolver

import fr.aphp.id.eds.requester.AppConfig

object FhirResourceResolverFactory {
  def getDefault: FhirResourceResolver = {
    AppConfig.get.defaultResolver match {
      case "solr" => new SolrQueryResolver(AppConfig.get.solr.get)
      case "fhir" => new RestFhirResolver(AppConfig.get.fhir.get)
      case _ => throw new IllegalArgumentException("No default resolver found")
    }
  }
}
