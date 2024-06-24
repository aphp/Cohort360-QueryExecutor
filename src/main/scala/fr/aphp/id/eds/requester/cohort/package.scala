package fr.aphp.id.eds.requester

package object cohort {
  object CohortCreationServices extends Enumeration {
    type CohortCreationServices = String
    val pg = "pg"
    val fhir = "fhir"
  }
}
