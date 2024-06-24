package fr.aphp.id.eds.requester.query

package object resolver {
  object ResourceResolvers extends Enumeration {
    type ResourceResolvers = String
    val solr = "solr"
    val fhir = "fhir"
  }
}
