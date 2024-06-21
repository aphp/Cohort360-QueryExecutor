package fr.aphp.id.eds.requester.query.resolver

import fr.aphp.id.eds.requester.AppConfig
import fr.aphp.id.eds.requester.query.resolver.rest.{DefaultRestFhirClient, RestFhirQueryElementsConfig, RestFhirResolver}
import fr.aphp.id.eds.requester.query.resolver.solr.{DefaultSolrSparkReader, SolrQueryElementsConfig, SolrQueryResolver, SolrSparkReader}

object ResourceResolverFactory {
  def get(resolver: Option[String] = None): ResourceResolver = {
    resolver.getOrElse(AppConfig.get.defaultResolver) match {
      case "solr" => {
        new SolrQueryResolver(new DefaultSolrSparkReader(AppConfig.get.solr.get))
      }
      case "fhir" => new RestFhirResolver(new DefaultRestFhirClient(AppConfig.get.fhir.get))
      case _ => throw new IllegalArgumentException("No default resolver found")
    }
  }

  def getConfig(resolver: Option[String] = None): ResourceConfig = {
    resolver.getOrElse(AppConfig.get.defaultResolver) match {
      case "solr" => new SolrQueryElementsConfig
      case "fhir" => new RestFhirQueryElementsConfig
      case _ => throw new IllegalArgumentException("No default config found")
    }
  }
}
