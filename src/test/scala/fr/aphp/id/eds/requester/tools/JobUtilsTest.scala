package fr.aphp.id.eds.requester.tools

import fr.aphp.id.eds.requester.query.model.{BasicResource, GroupResource, Request, SourcePopulation}
import fr.aphp.id.eds.requester.tools.JobUtils.initStageCounts
import org.scalatest.funsuite.AnyFunSuiteLike

class JobUtilsTest extends AnyFunSuiteLike {

  test("testInitStageCountsEmpty") {
    val modeOptions = Map[String,String]()
    val request = Request(
      sourcePopulation = SourcePopulation(None, None),
      request = None
    )
    assert(initStageCounts(modeOptions, request).isEmpty)
  }

  test("testInitStageAll") {
    val modeOptions = Map[String,String]("details" -> "all")
    val request = Request(
      sourcePopulation = SourcePopulation(None, None),
      request = Some(GroupResource(
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
    val stageCounts = initStageCounts(modeOptions, request)
    assert(stageCounts.isDefined)
    assert(stageCounts.get.size == 3)
    assert(stageCounts.get(1) == -1)
    assert(stageCounts.get(2) == -1)
    assert(stageCounts.get(3) == -1)

    val partialStageCounts = initStageCounts(Map("details" -> "2,3"), request)
    assert(partialStageCounts.isDefined)
    assert(partialStageCounts.get.size == 2)
    assert(partialStageCounts.get(2) == -1)
    assert(partialStageCounts.get(3) == -1)
  }


}
