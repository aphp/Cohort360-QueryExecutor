package fr.aphp.id.eds.requester.cohort.fhir

import fr.aphp.id.eds.requester.ResultColumn
import fr.aphp.id.eds.requester.cohort.CohortCreation
import fr.aphp.id.eds.requester.query.model.SourcePopulation
import fr.aphp.id.eds.requester.query.resolver.rest.RestFhirClient
import org.apache.spark.sql.{DataFrame, Row}
import org.hl7.fhir.r4.model.{ListResource, Reference}

import scala.jdk.CollectionConverters.seqAsJavaListConverter

class FhirCohortCreation(restFhirClient: RestFhirClient) extends CohortCreation {

  /**
    * @param cohortDefinitionName        : The name of the cohort
    * @param cohortDefinitionDescription : the full description of the cohort
    * @param cohortDefinitionSyntax      : the full json of the cohort
    * @param ownerEntityId               : the owner of the cohort
    * @param resourceType                : the type of the resource
    * @param size                        : the size of the cohort
    * @return the id of the newly created cohort
    */
  override def createCohort(cohortDefinitionName: String,
                            cohortDefinitionDescription: Option[String],
                            cohortDefinitionSyntax: String,
                            ownerEntityId: String,
                            resourceType: String,
                            size: Long): Long = {
    val list = new ListResource()
    list.setTitle(cohortDefinitionName)
    restFhirClient.getClient.create().resource(list).execute().getId.getIdPartAsLong
  }

  override def updateCohort(cohortId: Long,
                            cohort: DataFrame,
                            sourcePopulation: SourcePopulation,
                            count: Long,
                            delayCohortCreation: Boolean,
                            resourceType: String): Unit = {
    val list = restFhirClient.getClient
      .read()
      .resource(classOf[ListResource])
      .withId(cohortId.toString)
      .execute()

    list.setEntry(cohort.collect().map(r => createEntry(r)).toList.asJava)
    restFhirClient.getClient.update().resource(list).execute()
  }

  private def createEntry(row: Row): ListResource.ListEntryComponent = {
    val patient = new Reference()
    val patientId = row.getAs[String](ResultColumn.SUBJECT)
    patient.setReference("Patient/" + patientId)
    patient.setId(patientId)
    val entry = new ListResource.ListEntryComponent()
    entry.setItem(patient)
  }
}
