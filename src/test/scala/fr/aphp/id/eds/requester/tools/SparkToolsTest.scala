package fr.aphp.id.eds.requester.tools

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuiteLike

class SparkToolsTest extends AnyFunSuiteLike with DatasetComparer {
  System.setProperty("config.resource", "application.test.conf")
  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  test("cache") {
    val someDf = sparkSession.createDataFrame(Seq((1, "foo"), (2, "bar"))).toDF("id", "name")

    SparkTools.putCached("hello", "someuser", someDf)
    SparkTools.putCached("hello2", "someuser", someDf)

    val someOtherDf = sparkSession.createDataFrame(Seq((3, "baz"), (4, "qux"))).toDF("id", "name")
    SparkTools.putCached("hello", "someuser", someOtherDf)

    val overwrittenCachedDf = SparkTools.getCached(sparkSession, "hello", "someuser")
    val cachedDf = SparkTools.getCached(sparkSession, "hello2", "someuser")
    assert(cachedDf.isDefined)
    assertSmallDatasetEquality(cachedDf.get, someDf)
    assertSmallDatasetEquality(overwrittenCachedDf.get, someOtherDf)
    val cachedOtherDf = SparkTools.getCached(sparkSession, "hello", "someuser")
    assert(cachedOtherDf.isDefined)
    assertSmallDatasetEquality(cachedOtherDf.get, someOtherDf)

    SparkTools.getCached(sparkSession, "hello", "someuser2").isEmpty
  }

}
