package fr.aphp.id.eds.requester.query.resolver.rest

import fr.aphp.id.eds.requester.query.resolver.ResourceConfig
import fr.aphp.id.eds.requester.{FhirResource, QueryColumn}
import org.hl7.fhir.instance.model.api.IBase
import org.hl7.fhir.r4.model._

case class QueryColumnMapping(queryColName: String,
                              fhirPath: String,
                              fhirType: Class[_ <: IBase],
                              nullable: Boolean = true)

case class JoinInfo(resource: String, sourceJoinColumn: String)

case class ResourceMapping(columnMapping: QueryColumnMapping, joinInfo: Option[JoinInfo] = None)

class RestFhirQueryElementsConfig extends ResourceConfig {

  val fhirPathMappings: Map[String, List[ResourceMapping]] = Map(
    FhirResource.PATIENT -> List(
      ResourceMapping(QueryColumnMapping(QueryColumn.ID, "id", classOf[IdType])),
      ResourceMapping(QueryColumnMapping(QueryColumn.PATIENT, "id", classOf[IdType])),
      ResourceMapping(
        QueryColumnMapping(QueryColumn.PATIENT_BIRTHDATE, "birthDate", classOf[DateType])),
    ),
    FhirResource.ENCOUNTER -> addJoinedPatientResourceColumns(
      List(
        ResourceMapping(QueryColumnMapping(QueryColumn.ID, "id", classOf[IdType])),
        ResourceMapping(QueryColumnMapping(QueryColumn.ENCOUNTER, "id", classOf[IdType])),
        ResourceMapping(
          QueryColumnMapping(QueryColumn.EVENT_DATE, "period.start", classOf[DateTimeType])),
        ResourceMapping(QueryColumnMapping(QueryColumn.PATIENT, "subject", classOf[Reference])),
        ResourceMapping(
          QueryColumnMapping(QueryColumn.ENCOUNTER_START_DATE,
                             "period.start",
                             classOf[DateTimeType])),
        ResourceMapping(
          QueryColumnMapping(QueryColumn.ENCOUNTER_END_DATE, "period.end", classOf[DateTimeType])),
      )),
    FhirResource.OBSERVATION -> addJoinedResourceColumns(
      defaultResourceMapping(Some("subject"), Some("encounter"), Some("effectiveDateTime"), codeColumn = Some("code.coding.code"))),
    FhirResource.CONDITION -> addJoinedResourceColumns(
      defaultResourceMapping(Some("subject"), Some("encounter"), Some("recordedDate"), codeColumn = Some("code.coding.code"))),
    FhirResource.MEDICATION_REQUEST -> addJoinedResourceColumns(
      defaultResourceMapping(Some("subject"),
                             Some("encounter"),
                             Some("dispenseRequest.validityPeriod.start"), codeColumn = Some("medicationCodeableConcept.coding.code"))),
    FhirResource.MEDICATION_ADMINISTRATION -> addJoinedResourceColumns(
      defaultResourceMapping(Some("subject"), Some("encounter"), Some("effectivePeriod.start"), codeColumn = Some("medicationCodeableConcept.coding.code"))),
    FhirResource.DOCUMENT_REFERENCE -> addJoinedResourceColumns(
      defaultResourceMapping(Some("subject"), Some("encounter"), Some("date"))),
    FhirResource.CLAIM -> addJoinedResourceColumns(
      defaultResourceMapping(Some("subject"), Some("encounter"), Some("created"), codeColumn = Some("diagnosis.diagnosisCodeableConcept.coding.code"))),
    FhirResource.PROCEDURE -> addJoinedResourceColumns(
      defaultResourceMapping(Some("subject"), Some("encounter"), Some("date"), codeColumn = Some("code.coding.code"))),
    FhirResource.IMAGING_STUDY -> addJoinedResourceColumns(
      defaultResourceMapping(Some("subject"), Some("encounter"), Some("started"))),
    FhirResource.QUESTIONNAIRE_RESPONSE -> addJoinedResourceColumns(
      defaultResourceMapping(Some("patient"), Some("encounter"), Some("authored")))
  )

  override def requestKeyPerCollectionMap: Map[String, Map[String, List[String]]] = {
    fhirPathMappings.map {
      case (key, value) =>
        key -> value
          .map(mapping =>
            mapping.columnMapping.queryColName -> List(mapping.columnMapping.fhirPath))
          .toMap
    }
  }

  private def addJoinedResourceColumns(resourceMapping: List[ResourceMapping],
                                       queryColumnRef: String,
                                       resourceType: String,
                                       addedColumnsInfo: List[QueryColumnMapping]) = {
    resourceMapping.find(_.columnMapping.queryColName == queryColumnRef) match {
      case Some(ResourceMapping(baseColMapping, _)) =>
        resourceMapping ++ addedColumnsInfo.map { colMapping: QueryColumnMapping =>
          ResourceMapping(
            QueryColumnMapping(colMapping.queryColName,
                               s"${baseColMapping.fhirPath}.${colMapping.fhirPath}",
                               colMapping.fhirType,
                               colMapping.nullable),
            joinInfo = Some(JoinInfo(resourceType, baseColMapping.fhirPath))
          )
        }
      case _ => resourceMapping
    }
  }

  private def addJoinedPatientResourceColumns(
      resourceMapping: List[ResourceMapping]): List[ResourceMapping] = {
    addJoinedResourceColumns(
      resourceMapping,
      QueryColumn.PATIENT,
      FhirResource.PATIENT,
      List(
        QueryColumnMapping(QueryColumn.PATIENT_BIRTHDATE, "birthDate", classOf[DateType])
      ))
  }

  private def addJoinedEncounterResourceColumns(
      resourceMapping: List[ResourceMapping]): List[ResourceMapping] = {
    addJoinedResourceColumns(
      resourceMapping,
      QueryColumn.ENCOUNTER,
      FhirResource.ENCOUNTER,
      List(
        QueryColumnMapping(QueryColumn.ENCOUNTER_START_DATE, "period.start", classOf[DateTimeType]),
        QueryColumnMapping(QueryColumn.ENCOUNTER_END_DATE, "period.end", classOf[DateTimeType]),
        QueryColumnMapping(QueryColumn.EPISODE_OF_CARE, "episodeOfCare", classOf[Reference])
      )
    )
  }

  private def addJoinedResourceColumns(
      resourceMapping: List[ResourceMapping]): List[ResourceMapping] = {
    addJoinedEncounterResourceColumns(addJoinedPatientResourceColumns(resourceMapping))
  }

  private def defaultResourceMapping(patientColumn: Option[String] = Some("patient"),
                                     encounterColumn: Option[String] = Some("encounter"),
                                     eventColumn: Option[String] = None,
                                     codeColumn: Option[String] = None
                                    ): List[ResourceMapping] = {
    var resourceMappingList = List(
      ResourceMapping(QueryColumnMapping(QueryColumn.ID, "id", classOf[IdType]))
    )
    if (patientColumn.isDefined) {
      resourceMappingList = resourceMappingList :+ ResourceMapping(
        QueryColumnMapping(QueryColumn.PATIENT, patientColumn.get, classOf[Reference]))
    }
    if (encounterColumn.isDefined) {
      resourceMappingList = resourceMappingList :+ ResourceMapping(
        QueryColumnMapping(QueryColumn.ENCOUNTER, encounterColumn.get, classOf[Reference]))
    }
    if (eventColumn.isDefined) {
      resourceMappingList = resourceMappingList :+ ResourceMapping(
        QueryColumnMapping(QueryColumn.EVENT_DATE, eventColumn.get, classOf[DateTimeType]))
    }
    if (codeColumn.isDefined) {
      resourceMappingList = resourceMappingList :+ ResourceMapping(
        QueryColumnMapping(QueryColumn.CODE, codeColumn.get, classOf[CodeableConcept]))
    }
    resourceMappingList
  }

}
