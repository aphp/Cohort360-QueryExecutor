package fr.aphp.id.eds.requester.query

import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.List
import scala.io.Source

class QueryParserTest extends AnyFunSuiteLike {

  test("testParse") {
    val resource = QueryParser.parse(
      Source
        .fromFile(
          getClass.getResource("/testCases/temporalConstraintSameEncounter/request.json").getFile)
        .getLines
        .mkString)
    assert(resource._1.request.get.isInstanceOf[GroupResource])
    assert(
      resource._1.request.get.asInstanceOf[GroupResource].criteria.head.isInstanceOf[BasicResource])
    assert(resource._2(1).isDateTimeAvailable)
    assert(resource._2(1).isEncounterAvailable)
    assert(resource._2(1).isInTemporalConstraint)
    resource._2(1).requiredSolrFieldList should Matchers.equal(List("_visit"))
    assert(resource._2(1).temporalConstraintTypeList == List("sameEncounter"))
  }


  test("testParseWithOrganizations") {
    val resource = QueryParser.parse(
      Source
        .fromFile(
          getClass.getResource("/testCases/withOrganizationDetails/request.json").getFile)
        .getLines
        .mkString,
      QueryParsingOptions(withOrganizationDetails = true)
    )
    assert(resource._1.request.get.isInstanceOf[GroupResource])
    assert(resource._2.map((x) => x._2.withOrganizations).seq.forall((x) => x))
    assert(resource._2.filter((x) => List(1,3).contains(x._1)).head._2.requiredSolrFieldList == List("_list.organization", "_visit"))
  }

  test("testParseWithResourceFilter") {
    val resource = QueryParser.parse(
      Source
        .fromFile(
          getClass.getResource("/testCases/resourceCohort/request.json").getFile)
        .getLines
        .mkString
    )
    assert(resource._1.request.get.isInstanceOf[BasicResource])
    assert(resource._2.map((x) => x._2.isResourceFilter).seq.forall((x) => x))
    assert(resource._2.filter((x) => List(1).contains(x._1)).head._2.requiredSolrFieldList == List("id"))
  }

}
