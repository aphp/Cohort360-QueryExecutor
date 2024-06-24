package fr.aphp.id.eds.requester.query.resolver

abstract class ResourceConfig {

  def requestKeyPerCollectionMap: Map[String, Map[String, List[String]]]

}
