package fr.aphp.id.eds.requester.tools

import fr.aphp.id.eds.requester.query.model._
import fr.aphp.id.eds.requester.query.parser.CriterionTags
import fr.aphp.id.eds.requester.tools.JobUtils.{initStageDetails, prepareRequest}
import org.scalatest.funsuite.AnyFunSuiteLike

class JobUtilsTest extends AnyFunSuiteLike {

  test("testInitStageCountsEmpty") {
    val modeOptions = Map[String, String]()
    val request = Request(
      sourcePopulation = SourcePopulation(None),
      request = None
    )
    assert(initStageDetails(modeOptions, request).stageDfs.isEmpty)
    assert(initStageDetails(modeOptions, request).stageCounts.isEmpty)
  }

  test("testInitStageAll") {
    val modeOptions = Map[String, String]("details" -> "all")
    val request = Request(
      sourcePopulation = SourcePopulation(None),
      request = Some(
        GroupResource(
          groupType = "test",
          _id = 1,
          isInclusive = true,
          criteria = List(
            GroupResource(
              groupType = "test",
              _id = 2,
              isInclusive = true,
              criteria = List()
            ),
            BasicResource(
              _id = 3,
              isInclusive = true,
              resourceType = "test",
              filter = "test"
            )
          )
        ))
    )
    val stageCounts = initStageDetails(modeOptions, request)
    assert(stageCounts.stageCounts.isDefined)
    assert(stageCounts.stageDfs.isEmpty)
    assert(stageCounts.stageCounts.get.size == 3)
    assert(stageCounts.stageCounts.get(1) == -1)
    assert(stageCounts.stageCounts.get(2) == -1)
    assert(stageCounts.stageCounts.get(3) == -1)

    val stageRatios = initStageDetails(Map("details" -> "ratio"), request)
    assert(stageRatios.stageDfs.isDefined)
    assert(stageRatios.stageCounts.isEmpty)
    assert(stageRatios.stageDfs.get.isEmpty)

    val partialStageCounts = initStageDetails(Map("details" -> "2,3"), request)
    assert(partialStageCounts.stageCounts.isDefined)
    assert(stageCounts.stageDfs.isEmpty)
    assert(partialStageCounts.stageCounts.get.size == 2)
    assert(partialStageCounts.stageCounts.get(2) == -1)
    assert(partialStageCounts.stageCounts.get(3) == -1)
  }

  test("prepareRequest") {
    val request = Request(
      sourcePopulation = SourcePopulation(None),
      request = Some(
        GroupResource(
          groupType = GroupResourceType.AND,
          _id = 1,
          isInclusive = true,
          criteria = List(
            GroupResource(
              groupType = GroupResourceType.OR,
              _id = 2,
              isInclusive = true,
              criteria = List(
                GroupResource(
                  groupType = GroupResourceType.AND,
                  _id = 10,
                  isInclusive = false,
                  criteria = List(
                    BasicResource(
                      _id = 11,
                      isInclusive = true,
                      resourceType = "test",
                      filter = "testA"
                    ),
                    BasicResource(
                      _id = 12,
                      isInclusive = false,
                      resourceType = "test",
                      filter = "testB"
                    )
                  )
                ),
                BasicResource(_id = 4, isInclusive = false, resourceType = "test", filter = "testC"),
                GroupResource(
                  groupType = GroupResourceType.OR,
                  _id = 5,
                  isInclusive = true,
                  criteria = List(
                    BasicResource(
                      _id = 6,
                      isInclusive = true,
                      resourceType = "test",
                      filter = "testD"
                    ),
                    BasicResource(_id = 7,
                                  isInclusive = false,
                                  resourceType = "test",
                                  filter = "testE"),
                    GroupResource(
                      groupType = GroupResourceType.AND,
                      _id = 8,
                      isInclusive = true,
                      criteria = List(
                        BasicResource(
                          _id = 9,
                          isInclusive = false,
                          resourceType = "test",
                          filter = "test"
                        )
                      )
                    )
                  )
                )
              )
            ),
            BasicResource(
              _id = 3,
              isInclusive = true,
              resourceType = "test",
              filter = "test"
            )
          )
        ))
    )
    val dummyCriteriaTag = CriterionTags(
      isDateTimeAvailable = false,
      isEncounterAvailable = false,
      isEpisodeOfCareAvailable = false,
      isInTemporalConstraint = false,
      List[String](),
      "test",
      List[String]()
    )
    val criterionTagsMap = Map[Short, CriterionTags](
      1.toShort -> dummyCriteriaTag,
      2.toShort -> dummyCriteriaTag,
      3.toShort -> dummyCriteriaTag,
      4.toShort -> dummyCriteriaTag,
      5.toShort -> dummyCriteriaTag,
      6.toShort -> dummyCriteriaTag,
      7.toShort -> dummyCriteriaTag,
      8.toShort -> dummyCriteriaTag,
      9.toShort -> dummyCriteriaTag,
      10.toShort -> dummyCriteriaTag,
      11.toShort -> dummyCriteriaTag,
      12.toShort -> dummyCriteriaTag
    )
    val (preparedRequest, updatedCriterionTagsMap) =
      prepareRequest(request, criterionTagsMap)
    assert(updatedCriterionTagsMap.size == 15)
    var curGroup = preparedRequest.asInstanceOf[GroupResource]
    assert(curGroup.criteria.size == 2)
    curGroup = curGroup.criteria.head.asInstanceOf[GroupResource]
    assert(curGroup.criteria.size == 3)
    assert(curGroup.criteria.head.asInstanceOf[GroupResource].criteria.size == 1)
    assert(curGroup.criteria.head.asInstanceOf[GroupResource].i == 10)
    assert(curGroup.criteria.head.asInstanceOf[GroupResource].criteria.head.asInstanceOf[GroupResource].criteria.size == 2)
    assert(curGroup.criteria.head.asInstanceOf[GroupResource].criteria.head.asInstanceOf[GroupResource].criteria.head.asInstanceOf[BasicResource].filter == "testA")
    assert(curGroup.criteria.head.asInstanceOf[GroupResource].criteria.head.asInstanceOf[GroupResource].criteria(1).asInstanceOf[BasicResource].filter == "testB")
    assert(curGroup.criteria(1).asInstanceOf[GroupResource].criteria.size == 1)
    assert(curGroup.criteria(1).asInstanceOf[GroupResource].i == 4)
    assert(curGroup.criteria(1).asInstanceOf[GroupResource].criteria.head.asInstanceOf[BasicResource].filter == "testC")
    curGroup = curGroup.criteria(2).asInstanceOf[GroupResource]
    assert(curGroup.criteria.size == 3)
    assert(curGroup.criteria.head.asInstanceOf[BasicResource].filter == "testD")
    assert(curGroup.criteria(1).asInstanceOf[GroupResource].i == 7)
    assert(curGroup.criteria(1).asInstanceOf[GroupResource].criteria.size == 1)
    assert(curGroup.criteria(1).asInstanceOf[GroupResource].criteria.head.asInstanceOf[BasicResource].filter == "testE")
    assert(curGroup.criteria(2).asInstanceOf[GroupResource].i == 8)
    assert(curGroup.criteria(2).asInstanceOf[GroupResource].criteria.size == 1)
    assert(curGroup.criteria(2).asInstanceOf[GroupResource].criteria.head.asInstanceOf[BasicResource].filter == "test")
  }

}
