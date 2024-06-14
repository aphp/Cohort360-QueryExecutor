package fr.aphp.id.eds.requester.query.resolver.rest

import fr.aphp.id.eds.requester.query.resolver.QueryElementsConfig

class RestFhirQueryElementsConfig extends QueryElementsConfig{

  override def requestKeyPerCollectionMap: Map[String, Map[String, List[String]]] = Map(

  )

  override def reverseColumnMapping(collection: String, column_name: String): String = column_name
}
