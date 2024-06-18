package fr.aphp.id.eds.requester.query.resolver

import fr.aphp.id.eds.requester.AppConfig
import fr.aphp.id.eds.requester.query.resolver.rest.{DefaultRestFhirClient, RestFhirQueryElementsConfig, RestFhirResolver}
import fr.aphp.id.eds.requester.query.resolver.solr.{DefaultSolrSparkReader, SolrQueryElementsConfig, SolrQueryResolver, SolrSparkReader}

object ResourceResolverFactory {
  def getDefault: ResourceResolver = {
    AppConfig.get.defaultResolver match {
      case "solr" => {
        new SolrQueryResolver(new DefaultSolrSparkReader(AppConfig.get.solr.get))
      }
      case "fhir" => new RestFhirResolver(new DefaultRestFhirClient(AppConfig.get.fhir.get))
      case _ => throw new IllegalArgumentException("No default resolver found")
    }
  }

  def getDefaultConfig: ResourceConfig = {
    AppConfig.get.defaultResolver match {
      case "solr" => new SolrQueryElementsConfig
      case "fhir" => new RestFhirQueryElementsConfig
      case _ => throw new IllegalArgumentException("No default config found")
    }
  }
}
