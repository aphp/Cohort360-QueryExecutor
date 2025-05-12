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
  private val postgresContainer = new PostgreSQLContainer("postgres:15.3")

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .appName("Spark Unit Testing")
      .master("local[*]")
      .getOrCreate()
    tempDir = Files.createTempDirectory("test-temp-dir")
    postgresContainer.start()
    postgresContainer.withPassword("test")
    postgresContainer.withUsername("test")
    val pgPassFile = tempDir.resolve(".pgpass")
    Files.write(pgPassFile, s"${postgresContainer.getHost}:${postgresContainer.getFirstMappedPort}:*:${postgresContainer.getUsername}:${postgresContainer.getPassword}".getBytes)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteDirectory(tempDir.toFile)
    postgresContainer.stop()
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  test("testOutputBulk") {
    val spark = sparkSession
    import spark.implicits._
    val pgUrl = s"jdbc:postgresql://${postgresContainer.getHost}:${postgresContainer.getFirstMappedPort}/${postgresContainer.getDatabaseName}?user=${postgresContainer.getUsername}&currentSchema=public"
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
