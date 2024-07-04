package fr.aphp.id.eds.requester.server

import org.mockito.MockitoSugar.mock
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatra.swagger.Swagger

class JobControllerTest extends AnyFunSuiteLike {

  test("parseInputMinimal") {
    implicit val swagger: Swagger = mock[Swagger]
    val controller = new JobController()
    val input =
      """
        |{"input":{
        |  "cohortDefinitionSyntax": "some criteria",
        |  "ownerEntityId": "ownerId",
        |  "mode": "count",
        |}}
        |""".stripMargin
    val result = controller.parseInput(input)
    assert(result.mode == "count")
    assert(result.cohortDefinitionName == "Unnamed Cohort")
    assert(result.cohortDefinitionDescription.isEmpty)
    assert(result.cohortDefinitionSyntax == "some criteria")
    assert(result.ownerEntityId == "ownerId")
    assert(result.modeOptions.isEmpty)
    assert(result.resolver == "solr")
    assert(result.resolverOpts.isEmpty)
    assert(result.cohortCreationService == "pg")
    assert(result.solrRows == "10000")
    assert(result.commitWithin == "10000")
    assert(result.callbackPath.isEmpty)
    assert(result.callbackUrl.isEmpty)
    assert(result.cohortUuid.isEmpty)
  }

  test("parseInputAllOpts") {
    implicit val swagger: Swagger = mock[Swagger]
    val controller = new JobController()
    val input =
      """
        |{"input":{
        |  "cohortDefinitionName": "cohort name",
        |  "cohortDefinitionDescription": "some description",
        |  "cohortDefinitionSyntax": "some criteria",
        |  "ownerEntityId": "ownerId",
        |  "mode": "count",
        |  "modeOptions": {
        |    "opt1": "val1"
        |  },
        |  "resolver": "fhir",
        |  "resolverOpts": {
        |    "opt2": "val2"
        |  },
        |  "cohortCreationService": "fhir",
        |  "solrRows": "10000",
        |  "commitWithin": "10000"
        |  "callbackPath": "some/path",
        |  "callbackUrl": "http://some.url",
        |  "cohortUuid": "some-uuid"
        |}}
        |""".stripMargin
    val result = controller.parseInput(input)
    assert(result.mode == "count")
    assert(result.cohortDefinitionName == "cohort name")
    assert(result.cohortDefinitionDescription.contains("some description"))
    assert(result.cohortDefinitionSyntax == "some criteria")
    assert(result.ownerEntityId == "ownerId")
    assert(result.modeOptions.contains("opt1"))
    assert(result.resolver == "fhir")
    assert(result.resolverOpts.contains("opt2"))
    assert(result.cohortCreationService == "fhir")
    assert(result.solrRows == "10000")
    assert(result.commitWithin == "10000")
    assert(result.callbackPath.contains("some/path"))
    assert(result.callbackUrl.contains("http://some.url"))
    assert(result.cohortUuid.contains("some-uuid"))
  }

}
