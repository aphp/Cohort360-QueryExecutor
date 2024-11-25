package fr.aphp.id.eds.requester.cohort.pg

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import fr.aphp.id.eds.requester.query.model.SourcePopulation
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.mockito.{ArgumentCaptor, ArgumentMatchers, ArgumentMatchersSugar, MockitoSugar}
import org.mockito.MockitoSugar.{atLeast, mock, verify, verifyNoMoreInteractions, when}
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class PGCohortCreationTest extends AnyFunSuiteLike with DatasetComparer {
  System.setProperty("config.resource", "application.test.conf")

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
    pgCohortCreation.createCohort("test", Some("test"), "test", "test", "test", 1)
  }

  test("testUpdateCohort") {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val pgTools = mock[PGTool]

    val pgCohortCreation = new PGCohortCreation(pgTools)

    val cohortData = Seq(Tuple1(1), Tuple1(3), Tuple1(5))
    val cohort: DataFrame = sparkSession.createDataFrame(cohortData).toDF("subject_id")

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

    val expectedDf = sparkSession
      .createDataFrame(
        sparkSession.sparkContext.parallelize(
          List(Row(1, "Patient/1", "Cohort360", 12345L, -1238008758),
               Row(3, "Patient/3", "Cohort360", 12345L, -1332131217),
               Row(5, "Patient/5", "Cohort360", 12345L, 399554890))),
        StructType(
          StructField("_itemreferenceid", IntegerType, nullable = false) ::
            StructField("item__reference", StringType, nullable = false) ::
            StructField("_provider", StringType, nullable = false) ::
            StructField("_listid", LongType, nullable = false) ::
            StructField("hash", IntegerType, nullable = false) :: Nil
        )
      )
    val df = ArgumentCaptor.forClass(classOf[DataFrame])
    verify(pgTools, MockitoSugar.times(1)).outputBulk(
      ArgumentMatchers.eq("list__entry_cohort360"),
      df.capture(),
      ArgumentMatchers.eq(Some(4)),
      ArgumentMatchersSugar.*
    )
    assertSmallDatasetEquality(df.getValue.asInstanceOf[DataFrame], expectedDf)

    verify(pgTools, MockitoSugar.times(1)).purgeTmp()

    verifyNoMoreInteractions(pgTools)
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
