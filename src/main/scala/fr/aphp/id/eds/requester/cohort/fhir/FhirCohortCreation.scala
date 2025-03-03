package fr.aphp.id.eds.requester.cohort.fhir

import ca.uhn.fhir.rest.api.{SortOrderEnum, SortSpec}
import fr.aphp.id.eds.requester.ResultColumn
import fr.aphp.id.eds.requester.cohort.CohortCreation
import fr.aphp.id.eds.requester.query.model.SourcePopulation
import fr.aphp.id.eds.requester.query.resolver.rest.RestFhirClient
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.hl7.fhir.r4.model.ListResource.ListMode
import org.hl7.fhir.r4.model.{Bundle, Identifier, ListResource, Reference}

import scala.jdk.CollectionConverters.{asScalaBufferConverter, seqAsJavaListConverter}

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
                            baseCohortId: Option[Long],
                            mode: ListMode,
                            size: Long): Long = {
    val list = new ListResource()
    list.setTitle(cohortDefinitionName)
    list.setMode(mode)
    if (baseCohortId.isDefined) {
      list.setIdentifier(
        List(new Identifier().setValue(baseCohortId.get.toString)).asJava
      )
    }
    val fhirCreatedResource = restFhirClient.getClient.create().resource(list).execute()
    fhirCreatedResource.getResource.getIdElement.getIdPartAsLong
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

  override def readCohortEntries(cohortId: Long)(implicit spark: SparkSession): DataFrame = {
    val baseList = restFhirClient.getClient
      .read()
      .resource(classOf[ListResource])
      .withId(cohortId.toString)
      .execute()
      .getEntry

    val diffListsResults: Bundle = restFhirClient.getClient
      .search()
      .forResource(classOf[ListResource])
      .where(ListResource.IDENTIFIER.exactly().code(cohortId.toString))
      .sort(new SortSpec("date", SortOrderEnum.ASC))
      .execute()
    val diffLists = diffListsResults.getEntry.asScala
      .map(_.getResource.asInstanceOf[ListResource])
      .filter(l => l.hasMode && l.getMode.equals(ListMode.CHANGES))

    val diffEntries = diffLists.flatMap(_.getEntry.asScala)
    val result = diffEntries.foldLeft(baseList.asScala.map(_.getItem.getReference).toSet) {
      (acc, entry) =>
        val itemId = entry.getItem.getReference
        val deleted = entry.getDeleted
        if (deleted) {
          acc - itemId
        } else {
          acc + itemId
        }
    }

    import spark.implicits._

    result.toSeq.map(id => id.split("/").last.toLong).toDF("_itemreferenceid")
  }

  private def createEntry(row: Row): ListResource.ListEntryComponent = {
    val patient = new Reference()
    val patientId = row.getAs[Long](ResultColumn.SUBJECT)
    val deleted = row.getAs[Boolean]("deleted")
    patient.setReference("Patient/" + patientId)
    patient.setId(patientId.toString)
    val entry = new ListResource.ListEntryComponent()
    entry.setItem(patient)
    entry.setDeleted(deleted)
    entry
  }
}
