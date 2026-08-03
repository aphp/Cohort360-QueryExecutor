package fr.aphp.id.eds.requester.cohort.pg

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.PostgreSQLContainer
import org.scalatest.funsuite.AnyFunSuiteLike

import java.nio.file.{Files, Path}

class PGToolTest extends AnyFunSuiteLike with Matchers with BeforeAndAfterAll with BeforeAndAfter {
  var sparkSession: SparkSession = _

  private var tempDir: java.nio.file.Path = _

  private val externalHost = sys.env.get("PG_TEST_HOST")
  private val postgresContainer =
    if (externalHost.isDefined) None else Some(new PostgreSQLContainer("postgres:15.3"))

  private def pgHost: String = externalHost.getOrElse(postgresContainer.get.getHost)
  private def pgPort: String =
    sys.env.getOrElse("PG_TEST_PORT", postgresContainer.get.getFirstMappedPort.toString)
  private def pgDb: String =
    sys.env.getOrElse("PG_TEST_DB", postgresContainer.get.getDatabaseName)
  private def pgUser: String =
    sys.env.getOrElse("PG_TEST_USER", postgresContainer.get.getUsername)
  private def pgPassword: String =
    sys.env.getOrElse("PG_TEST_PASSWORD", postgresContainer.get.getPassword)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .appName("Spark Unit Testing")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    tempDir = Files.createTempDirectory("test-temp-dir")
    postgresContainer.foreach(_.start())
    val pgPassFile = tempDir.resolve(".pgpass")
    Files.write(pgPassFile, s"$pgHost:$pgPort:*:$pgUser:$pgPassword".getBytes)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteDirectory(tempDir.toFile)
    postgresContainer.foreach(_.stop())
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  test("testOutputBulk") {
    val spark = sparkSession
    import spark.implicits._
    val pgUrl = s"jdbc:postgresql://$pgHost:$pgPort/$pgDb?user=$pgUser&currentSchema=public"
    val pgTool = PGTool(sparkSession, pgUrl, tempDir.toString, pgPassFile = new org.apache.hadoop.fs.Path(tempDir.resolve(".pgpass").toString))
    val createTableQuery = """
      CREATE TABLE test_table (
        id INT PRIMARY KEY,
        value TEXT,
        id_2 INT
      )
    """
    pgTool.sqlExec(createTableQuery)

    val insertDataQuery = """
      INSERT INTO test_table (id, value, id_2) VALUES
      (1, '1', 1),
      (2, '2', 2)
    """
    pgTool.sqlExec(insertDataQuery)
    val baseContent = pgTool.sqlExecWithResult("select * from test_table")
    baseContent.collect().map(_.getInt(0)) should contain theSameElementsAs Array(1, 2)

    // generate a new dataframe containing 100 elements with 2 columns id and value that will be written to the database
    val data = sparkSession.range(100).toDF("id").withColumn("value", 'id.cast("string")).withColumn("id_2", col("id"))
    pgTool.outputBulk("test_table", data, primaryKeys = Seq("id", "id_2"))
    val updatedContent = pgTool.sqlExecWithResult("select * from test_table")
    updatedContent.collect().map(_.getInt(0)) should contain theSameElementsAs (0 until 100)
  }

}
