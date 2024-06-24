package fr.aphp.id.eds.requester.query.resolver

import fr.aphp.id.eds.requester.AppConfig
import fr.aphp.id.eds.requester.query.resolver.ResourceResolvers.ResourceResolvers
import fr.aphp.id.eds.requester.query.resolver.rest.{DefaultRestFhirClient, RestFhirQueryElementsConfig, RestFhirResolver}
import fr.aphp.id.eds.requester.query.resolver.solr.{DefaultSolrSparkReader, SolrQueryElementsConfig, SolrQueryResolver, SolrSparkReader}

object ResourceResolverFactory {
  def get(resolver: ResourceResolvers): ResourceResolver = {
    resolver match {
      case ResourceResolvers.solr => {
        new SolrQueryResolver(new DefaultSolrSparkReader(AppConfig.get.solr.get))
      }
      case ResourceResolvers.fhir => new RestFhirResolver(new DefaultRestFhirClient(AppConfig.get.fhir.get))
      case _ => throw new IllegalArgumentException("Unknown resolver type")
    }
  }
}
