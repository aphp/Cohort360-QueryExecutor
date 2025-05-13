package fr.aphp.id.eds.requester.cohort.pg

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import fr.aphp.id.eds.requester.query.model.SourcePopulation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.hl7.fhir.r4.model.ListResource.ListMode
import org.mockito.{ArgumentCaptor, ArgumentMatchers, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class PGCohortCreationTest
    extends AnyFunSuiteLike
    with DatasetComparer
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll {
  System.setProperty("config.resource", "application.test.conf")

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .appName("PGCohortCreationTest")
      .getOrCreate()
  }

  test("testCreateCohort") {
    val pgTools = mock[PGTool]
    val pgCohortCreation = new PGCohortCreation(pgTools)
    val expectedResult: Dataset[Row] = mock[Dataset[Row]]
    when(expectedResult.collect()).thenReturn(Array(Row(1L)))
    when(
      pgTools.sqlExecWithResult(
        """
        |insert into list_cohort360
        |(hash, title, note__text, _sourcereferenceid, source__reference, _provider, source__type, mode, status, subject__type, date, _size)
        |values (-1, ?, ?, ?, ?, 'Cohort360', 'Practitioner', 'snapshot', 'retired', ?, now(), ?)
        |returning id
        |""".stripMargin,
        List("test", "test", "test", "Practitioner/test", "test", 1)
      )).thenReturn(expectedResult)
    pgCohortCreation.createCohort("test",
                                  Some("test"),
                                  "test",
                                  "test",
                                  "test",
                                  None,
                                  ListMode.SNAPSHOT,
                                  1)
  }

  test("testUpdateCohort") {
    val pgTools = mock[PGTool]

    val pgCohortCreation = new PGCohortCreation(pgTools)

    val cohortData = Seq(Tuple2(1, false), Tuple2(3, false), Tuple2(5, false))
    val cohort: DataFrame = spark
      .createDataFrame(cohortData)
      .toDF("subject_id", "deleted")
      .withColumn("deleted", col("deleted").cast(BooleanType))

    pgCohortCreation.updateCohort(12345,
                                  cohort,
                                  SourcePopulation(Some(List(888))),
                                  3,
                                  delayCohortCreation = true,
                                  "Patient")
    val sqlStmts = ArgumentCaptor.forClass(classOf[String])
    val sqlParams = ArgumentCaptor.forClass(classOf[List[Any]])
    verify(pgTools, atLeast(1)).sqlExec(
      sqlStmts.capture(),
      sqlParams.capture()
    )
    compareStringList(
      sqlStmts.getAllValues.toList.asInstanceOf[List[String]],
      List(
        """
        |update list_cohort360
        |set _size = 3
        |where id = 12345
        |""".stripMargin,
        """
        |update list_cohort360
        |set status = 'current'
        |where id = 12345
        """.stripMargin,
        """
        |update list_cohort360
        |set mode = 'working'
        |where id = 12345
        """.stripMargin,
        """
        |insert into fact_relationship
        |(hash, insert_datetime, change_datetime, domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id, cdm_source)
        |values (-1, now(), now(), 1147323, ?, 1147323, ?, 44818821, 'Cohort360')
        """.stripMargin,
        """
        |insert into fact_relationship
        |(hash, insert_datetime, change_datetime, domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id, cdm_source)
        |values (-1, now(), now(), 1147323, ?, 1147323, ?, 44818823, 'Cohort360')
        """.stripMargin,
      )
    )
    compareList(sqlParams.getAllValues.toList.asInstanceOf[List[Any]],
                List(
                  List(),
                  List(),
                  List(),
                  List(12345, 888),
                  List(888, 12345)
                ))

    val expectedDf = spark
      .createDataFrame(
        spark.sparkContext.parallelize(
          List(
            Row(1, "Patient/1", "Cohort360", 12345L, false, -1662328687),
            Row(3, "Patient/3", "Cohort360", 12345L, false, -1015512107),
            Row(5, "Patient/5", "Cohort360", 12345L, false, -1473784149)
          )),
        StructType(
          StructField("_itemreferenceid", IntegerType, nullable = false) ::
            StructField("item__reference", StringType, nullable = false) ::
            StructField("_provider", StringType, nullable = false) ::
            StructField("_listid", LongType, nullable = false) ::
            StructField("deleted", BooleanType, nullable = false) ::
            StructField("hash", IntegerType, nullable = false) :: Nil
        )
      )
    val df = ArgumentCaptor.forClass(classOf[DataFrame])
    verify(pgTools, MockitoSugar.times(1)).outputBulk(
      ArgumentMatchers.eq("list__entry_cohort360"),
      df.capture(),
      ArgumentMatchers.eq(Some(4)),
      ArgumentMatchersSugar.*,
      ArgumentMatchersSugar.*
    )
    assertSmallDatasetEquality(df.getValue.asInstanceOf[DataFrame], expectedDf)

    verify(pgTools, MockitoSugar.times(1)).purgeTmp()

    verifyNoMoreInteractions(pgTools)
  }

  test("readCohortEntries") {
    // Mock PGTool
    val mockPGTool = mock[PGTool]

    val cohortId = 123L

    // Create base cohort DataFrame
    val baseCohortSchema = StructType(
      Seq(
        StructField("_itemreferenceid", StringType, nullable = false)
      ))

    val baseCohortData = Seq(
      Row("patient1"),
      Row("patient2"),
      Row("patient3"),
      Row("patient4")
    )

    val baseCohortDf = spark.createDataFrame(
      spark.sparkContext.parallelize(baseCohortData),
      baseCohortSchema
    )

    // Create diff DataFrame with additions and deletions
    val diffSchema = StructType(
      Seq(
        StructField("date", TimestampType, nullable = false),
        StructField("_itemreferenceid", StringType, nullable = false),
        StructField("deleted", BooleanType, nullable = true)
      ))

    import java.sql.Timestamp
    import java.time.Instant

    val now = Timestamp.from(Instant.now())
    val earlier = Timestamp.from(Instant.now().minusSeconds(3600))

    val diffData = Seq(
      // Patient4 was deleted
      Row(now, "patient4", true),
      // Patient5 was added
      Row(now, "patient5", null),
      // Patient6 was added and then deleted (should not be included)
      Row(earlier, "patient6", null),
      Row(now, "patient6", true),
      // Patient7 was deleted and then added (should be included)
      Row(earlier, "patient7", true),
      Row(now, "patient7", false)
    )

    val diffDf = spark.createDataFrame(
      spark.sparkContext.parallelize(diffData),
      diffSchema
    )

    // Configure mock behavior
    when(mockPGTool.sqlExecWithResult(ArgumentMatchers.contains("select _itemreferenceid"), ArgumentMatchers.any()))
      .thenReturn(baseCohortDf)

    when(
      mockPGTool.sqlExecWithResult(
        ArgumentMatchers.contains("select date,_itemreferenceid,deleted"), ArgumentMatchers.any()))
      .thenReturn(diffDf)

    // Create the cohort creation instance with our mock
    val cohortCreation = new PGCohortCreation(mockPGTool)

    // Call the method under test
    val result = cohortCreation.readCohortEntries(cohortId)

    // Expected result: base cohort minus deletions plus additions
    // Expected: patient1, patient2, patient3, patient5, patient7
    // Not expected: patient4 (deleted), patient6 (added then deleted)

    val expectedIds = Set("patient1", "patient2", "patient3", "patient5", "patient7")

    // Verify the result
    val resultIds = result.collect().map(row => row.getAs[String]("_itemreferenceid")).toSet
    resultIds should be(expectedIds)

    // Verify that the sqlExecWithResult was called twice
    verify(mockPGTool, times(1))
      .sqlExecWithResult(ArgumentMatchers.contains("select _itemreferenceid"), ArgumentMatchers.any())
    verify(mockPGTool, times(1))
      .sqlExecWithResult(ArgumentMatchers.contains("select date,_itemreferenceid,deleted"), ArgumentMatchers.any())
  }


  def compareList(actual: List[Any], expected: List[Any]): Unit = {
    assert(actual.size == expected.size)
    for (i <- actual.indices) {
      assert(actual(i) == expected(i))
    }
  }

  def compareStringList(actual: List[String], expected: List[String]): Unit = {
    assert(actual.size == expected.size)
    for (i <- actual.indices) {
      assert(actual(i).stripMargin.stripTrailing() == expected(i).stripMargin.stripTrailing())
    }
  }
}
