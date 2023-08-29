package fr.aphp.id.eds.requester

import fr.aphp.id.eds.requester.jobs.SparkJobParameter
import org.scalatest.funsuite.AnyFunSuiteLike

class CountQueryTest extends AnyFunSuiteLike {

  System.setProperty("config.resource", "application.test.conf")

  test("testCallbackUrl") {
    var callbackUrl = CountQuery.callbackUrl(SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test", Some("test"), Some("url")))
    assert(callbackUrl.isDefined)
    assert(callbackUrl.get == "url")
    callbackUrl = CountQuery.callbackUrl(SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test", Some("id")))
    assert(callbackUrl.isDefined)
    assert(callbackUrl.get == "http://django/cohort/dated-measures/id/")
    callbackUrl = CountQuery.callbackUrl(SparkJobParameter("test", Some("test"), "test", "test", "test", "test", "test"))
    assert(callbackUrl.isEmpty)
  }

  test("testRunJob") {}

}

