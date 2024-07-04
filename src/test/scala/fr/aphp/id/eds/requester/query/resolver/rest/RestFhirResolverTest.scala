package fr.aphp.id.eds.requester.query.resolver.rest

import ca.uhn.fhir.context.FhirContext
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import fr.aphp.id.eds.requester.FhirResource
import fr.aphp.id.eds.requester.query.model.{BasicResource, SourcePopulation}
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.hl7.fhir.r4.model.Bundle
import org.mockito.Mockito.when
import org.mockito.MockitoSugar.mock
import org.scalatest.funsuite.AnyFunSuiteLike

class RestFhirResolverTest extends AnyFunSuiteLike with DatasetComparer {
  System.setProperty("config.resource", "application.test.conf")
  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.session.timeZone", "Europe/Paris")
    .getOrCreate()

  def mockGetBundle(restFhirClient: RestFhirClient,
                    dataPath: String,
                    resourceType: String,
                    filter: String,
                    elements: List[String]): Unit = {
    val ctx = FhirContext.forR4
    val parser = ctx.newJsonParser
    val resource = getClass.getResource(dataPath)
    val resourceJson = scala.io.Source.fromFile(resource.getPath).mkString
    val bundle = parser.parseResource(resourceJson).asInstanceOf[Bundle]
    when(restFhirClient.getBundle(resourceType, filter, elements)).thenReturn(bundle)
  }

  // TODO verify that mocks are really called
  def testCase(folderCase: String,
               basicResource: BasicResource,
               requiredFieldList: List[String],
               fhirClientMocks: RestFhirClient => Unit = (_) => {},
               patchExpected: DataFrame => DataFrame = (x: DataFrame) => { x }): Unit = {
    val restFhirClient = mock[RestFhirClient]
    when(restFhirClient.getFhirContext).thenReturn(FhirContext.forR4())
    val restFhirResolver = new RestFhirResolver(restFhirClient)

    val expected = getClass.getResource(s"/resolver/restfhir/testCases/$folderCase/expected.csv")

    fhirClientMocks(restFhirClient)

    val expectedResult = patchExpected(
      sparkSession.read
        .format("csv")
        .option("delimiter", ";")
        .option("header", "true")
        .load(expected.getPath))

    val criterionTags = new CriterionTags(true,
                                          true,
                                          false,
                                          false,
                                          resourceType = basicResource.resourceType,
                                          requiredFieldList = requiredFieldList)
    val sourcePopulation = SourcePopulation(
      caresiteCohortList = Some(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
      providerCohortList = None
    )
    val resourceDataFrame =
      restFhirResolver.getResourceDataFrame(basicResource, criterionTags, sourcePopulation)
    assert(resourceDataFrame.isInstanceOf[DataFrame])
    assertSmallDatasetEquality(resourceDataFrame, expectedResult, orderedComparison = false)
  }

  test("simple") {
    testCase(
      "simple",
      BasicResource(1, isInclusive = true, FhirResource.MEDICATION_REQUEST, "medication=AA"),
      List("id", "subject"),
      (restFhirClient: RestFhirClient) => {
        mockGetBundle(
          restFhirClient,
          "/resolver/restfhir/testCases/simple/bundle.json",
          FhirResource.MEDICATION_REQUEST,
          "medication=AA&_list=1,2,3,4,5,6,7,8,9,10&_count=1000",
          List("id", "subject")
        )
      }
    )
  }

  test("getMultiplePages") {
    testCase(
      "getMultiplePages",
      BasicResource(1, isInclusive = true, FhirResource.MEDICATION_REQUEST, "medication=AA"),
      List("id", "encounter"),
      (restFhirClient) => {
        val ctx = FhirContext.forR4
        val parser = ctx.newJsonParser
        val resource =
          getClass.getResource("/resolver/restfhir/testCases/getMultiplePages/bundle.json")
        val resourceJson = scala.io.Source.fromFile(resource.getPath).mkString
        val bundle = parser.parseResource(resourceJson).asInstanceOf[Bundle]
        when(
          restFhirClient.getBundle(FhirResource.MEDICATION_REQUEST,
                                   "medication=AA&_list=1,2,3,4,5,6,7,8,9,10&_count=1000",
                                   List("id", "encounter", "subject"))).thenReturn(bundle)

        val resourceNext =
          getClass.getResource("/resolver/restfhir/testCases/getMultiplePages/bundle.next.json")
        val resourceNextJson = scala.io.Source.fromFile(resourceNext.getPath).mkString
        val bundleNext = parser.parseResource(resourceNextJson).asInstanceOf[Bundle]
        when(restFhirClient.getNextPage(bundle)).thenReturn(bundleNext)
      }
    )
  }

  test("withPatientJoinResource") {
    testCase(
      "withPatientJoinResource",
      BasicResource(1, isInclusive = true, FhirResource.OBSERVATION, "code=AA"),
      List("id", "subject.birthDate"),
      (restFhirClient) => {
        mockGetBundle(
          restFhirClient,
          "/resolver/restfhir/testCases/withPatientJoinResource/bundle.observation.json",
          FhirResource.OBSERVATION,
          "code=AA&_list=1,2,3,4,5,6,7,8,9,10&_count=1000",
          List("id", "subject")
        )
        mockGetBundle(
          restFhirClient,
          "/resolver/restfhir/testCases/withPatientJoinResource/bundle.patient.json",
          FhirResource.PATIENT,
          "_id=1566947,1900536&_count=1000&_list=1,2,3,4,5,6,7,8,9,10",
          List("birthDate", "id")
        )
      },
      (expected: DataFrame) => {
        expected.withColumn("patient_birthdate", expected("patient_birthdate").cast(DateType))
      }
    )
  }

  test("withEncounterJoinResource") {
    testCase(
      "withEncounterJoinResource",
      BasicResource(1, isInclusive = true, FhirResource.IMAGING_STUDY, "modality=AA"),
      List("id", "encounter.period.start", "encounter.period.end"),
      (restFhirClient) => {
        mockGetBundle(
          restFhirClient,
          "/resolver/restfhir/testCases/withEncounterJoinResource/bundle.imaging.json",
          FhirResource.IMAGING_STUDY,
          "modality=AA&_list=1,2,3,4,5,6,7,8,9,10&_count=1000",
          List("id", "encounter", "subject")
        )
        mockGetBundle(
          restFhirClient,
          "/resolver/restfhir/testCases/withEncounterJoinResource/bundle.encounter.json",
          FhirResource.ENCOUNTER,
          "_id=6042,6131,6192,6253,6314,6396,6467,6528,6589,6650,6711,6772,6833,6894,6955,7016,7077,7138,7199,7260&_count=1000&_list=1,2,3,4,5,6,7,8,9,10",
          List("period", "id")
        )
      },
      (expected: DataFrame) => {
        expected
          .withColumn("encounter_start_date", expected("encounter_start_date").cast(TimestampType))
          .withColumn("encounter_end_date", expected("encounter_end_date").cast(TimestampType))
      }
    )
  }

  test("countPatients") {
    val restFhirClient = mock[RestFhirClient]
    when(restFhirClient.getFhirContext).thenReturn(FhirContext.forR4())
    val restFhirResolver = new RestFhirResolver(restFhirClient)

    val ctx = FhirContext.forR4
    val parser = ctx.newJsonParser
    val resource = getClass.getResource("/resolver/restfhir/testCases/countPatients/bundle.json")
    val resourceJson = scala.io.Source.fromFile(resource.getPath).mkString
    val bundle = parser.parseResource(resourceJson).asInstanceOf[Bundle]
    when(
      restFhirClient.getBundle(
        FhirResource.PATIENT,
        "active=true&_security:not=http%3A%2F%2Fterminology.hl7.org%2FCodeSystem%2Fv3-ActCode%7CNOLIST&_list=1,2,3,4,5,6,7,8,9,10&_count=0",
        List()
      )).thenReturn(bundle)

    val sourcePopulation = SourcePopulation(
      caresiteCohortList = Some(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
      providerCohortList = None
    )
    val count = restFhirResolver.countPatients(sourcePopulation)
    assert(count == 1996)
  }

}
