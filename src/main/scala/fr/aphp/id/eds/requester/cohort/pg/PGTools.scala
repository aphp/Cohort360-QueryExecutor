package fr.aphp.id.eds.requester.cohort.pg

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Partitioner
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import java.io._
import java.sql._
import java.util.Properties
import java.util.UUID.randomUUID

sealed trait BulkLoadMode

object CSV extends BulkLoadMode

object Stream extends BulkLoadMode

object PgBinaryStream extends BulkLoadMode

object PgBinaryFiles extends BulkLoadMode

class PGTool(
    spark: SparkSession,
    url: String,
    tmpPath: String,
    bulkLoadMode: BulkLoadMode,
    bulkLoadBufferSize: Int
) {

  private var password: String = ""

  def setPassword(pwd: String = ""): PGTool = {
    password = PGTool.passwordFromConn(url, pwd)
    this
  }

  def purgeTmp(): Boolean = {
    val defaultFSConf = spark.sessionState.newHadoopConf().get("fs.defaultFS")
    val fsConf = if (tmpPath.startsWith("file:")) {
      "file:///"
    } else {
      defaultFSConf
    }
    val conf = new Configuration()
    conf.set("fs.defaultFS", fsConf)
    val fs = FileSystem.get(conf)
    fs.deleteOnExit(new Path(tmpPath)) // delete file when spark quits
  }

  def sqlExec(query: String, params: List[Any] = Nil): PGTool = {
    PGTool.sqlExec(url, query, password, params)
    this
  }

  def sqlExecWithResult(
      query: String,
      params: List[Any] = Nil
  ): Dataset[Row] = {
    PGTool.sqlExecWithResult(spark, url, query, password, params)
  }

  /**
    * Write a spark dataframe into a postgres table using
    * the built-in COPY
    */
  def outputBulk(
      table: String,
      df: Dataset[Row],
      numPartitions: Option[Int] = None,
      reindex: Boolean = false
  ): PGTool = {
    PGTool.outputBulk(
      spark,
      url,
      table,
      df,
      genPath,
      numPartitions.getOrElse(8),
      password,
      reindex,
      bulkLoadBufferSize
    )
    this
  }

  private def genPath: String = {
    tmpPath + "/" + randomUUID.toString
  }

}

object PGTool extends java.io.Serializable with LazyLogging {

  val defaultBulkLoadStrategy: BulkLoadMode = CSV
  val defaultBulkLoadBufferSize: Int = 512 * 1024
  val defaultStreamBulkLoadTimeoutMs: Int = 10 * 64 * 1000

  def apply(
      spark: SparkSession,
      url: String,
      tmpPath: String,
      bulkLoadMode: BulkLoadMode = defaultBulkLoadStrategy,
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize
  ): PGTool = {
    new PGTool(
      spark,
      url,
      tmpPath + "/spark-postgres-" + randomUUID.toString,
      bulkLoadMode,
      bulkLoadBufferSize
    ).setPassword()
  }

  def connOpen(url: String, password: String = ""): Connection = {
    val prop = new Properties()
    prop.put("driver", "org.postgresql.Driver")
    prop.put("password", passwordFromConn(url, password))
    DriverManager.getConnection(url, prop)
  }

  def passwordFromConn(url: String, password: String): String = {
    if (password.nonEmpty) {
      return password
    }
    val pattern = "jdbc:postgresql://(.*):(\\d+)/(\\w+)[?]user=(\\w+).*".r
    val pattern(host, port, database, username) = url
    dbPassword(host, port, database, username)
  }

  private def dbPassword(
      hostname: String,
      port: String,
      database: String,
      username: String
  ): String = {
    // Usage: val thatPassWord = dbPassword(hostname,port,database,username)
    // .pgpass file format, hostname:port:database:username:password

    val fs = FileSystem.get(new java.net.URI("file:///"), new Configuration)
    val reader = new BufferedReader(
      new InputStreamReader(fs.open(new Path(scala.sys.env("HOME"), ".pgpass")))
    )
    val content = Iterator
      .continually(reader.readLine())
      .takeWhile(_ != null)
      .mkString("\n")
    var passwd = ""
    content.split("\n").foreach { line =>
      val connCfg = line.split(":")
      if (hostname == connCfg(0)
          && port == connCfg(1)
          && (database == connCfg(2) || connCfg(2) == "*")
          && username == connCfg(3)) {
        passwd = connCfg(4)
      }
    }
    reader.close()
    passwd
  }

  def sqlExec(
      url: String,
      query: String,
      password: String = "",
      params: List[Any] = Nil
  ): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(query)
    parametrize(st, params).executeUpdate()
    conn.close()
  }

  def outputBulk(
      spark: SparkSession,
      url: String,
      table: String,
      df: Dataset[Row],
      path: String,
      numPartitions: Int = 8,
      password: String = "",
      reindex: Boolean = false,
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize
  ): Unit = {
    logger.debug("using CSV strategy")
    try {
      if (reindex)
        indexDeactivate(url, table, password)
      val columns = df.schema.fields.map(x => s"${sanP(x.name)}").mkString(",")
      //transform arrays to string
      val dfTmp = dataframeToPgCsv(df)
      //write a csv folder
      dfTmp.write
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .option("nullValue", null)
        .option("emptyValue", "\"\"")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .save(path)

      outputBulkCsvLow(
        spark,
        url,
        table,
        columns,
        path,
        numPartitions,
        ",",
        ".*.csv",
        password,
        bulkLoadBufferSize
      )
    } finally {
      if (reindex)
        indexReactivate(url, table, password)
    }
  }

  def outputBulkFileLow(
      spark: SparkSession,
      url: String,
      path: String,
      sqlCopy: String,
      extensionPattern: String,
      numPartitions: Int = 8,
      password: String = "",
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize
  ): Unit = {

    // load the csv files from hdfs in parallel
    val fs = FileSystem.get(new Configuration())
    import spark.implicits._
    val rdd = fs
      .listStatus(new Path(path))
      .filter(x => x.getPath.toString.matches("^.*/" + extensionPattern + "$"))
      .map(x => x.getPath.toString)
      .toList
      .zipWithIndex
      .map(_.swap)
      .toDS
      .rdd
      .partitionBy(new ExactPartitioner(numPartitions))

    rdd.foreachPartition(x => {
      val conn = connOpen(url, password)
      x.foreach { s =>
        {
          val stream: InputStream = FileSystem
            .get(new Configuration())
            .open(new Path(s._2))
            .getWrappedStream
          val copyManager: CopyManager =
            new CopyManager(conn.asInstanceOf[BaseConnection])
          copyManager.copyIn(sqlCopy, stream, bulkLoadBufferSize)
        }
      }
      conn.close()
      x.toIterator
    })
  }

  def outputBulkCsvLow(
      spark: SparkSession,
      url: String,
      table: String,
      columns: String,
      path: String,
      numPartitions: Int = 8,
      delimiter: String = ",",
      extensionPattern: String = ".*.csv",
      password: String = "",
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize
  ): Unit = {
    val csvSqlCopy =
      s"""COPY "$table" ($columns) FROM STDIN WITH CSV DELIMITER '$delimiter'  NULL '' ESCAPE '"' QUOTE '"' """
    outputBulkFileLow(
      spark,
      url,
      path,
      csvSqlCopy,
      extensionPattern,
      numPartitions,
      password,
      bulkLoadBufferSize
    )
  }

  def getSchema(url: String): String = {
    val pattern = "jdbc:postgresql://.+?&currentSchema=(\\w+)".r
    val pattern(schema) = url
    schema
  }

  def indexDeactivate(url: String, table: String, password: String = ""): Unit = {
    val schema = getSchema(url)
    val query =
      s"""
    UPDATE pg_index
    SET indisready = false
    WHERE indrelid IN (
    SELECT pg_class.oid FROM pg_class
    JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
    WHERE relname='$table' and nspname = '$schema' )
    """
    sqlExec(url, query, password)
    logger.debug(s"Deactivating indexes from $schema.$table")
  }

  def indexReactivate(url: String, table: String, password: String = ""): Unit = {

    val schema = getSchema(url)
    val query =
      s"""
      UPDATE pg_index
      SET indisready = true
      WHERE indrelid IN (
      SELECT pg_class.oid FROM pg_class
      JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
      WHERE relname='$table' and nspname = '$schema' )
    """
    sqlExec(url, query, password)
    logger.debug(s"Reactivating indexes from $schema.$table")

    val query2 =
      s"""
      REINDEX TABLE "$schema"."$table"
    """

    sqlExec(url, query2, password)
  }

  def sanP(obj: String): String = obj.mkString("\"", "", "\"")

  def dataframeToPgCsv(df: Dataset[Row]): DataFrame = {
    val newCols = df.schema.fields.map(c => {
      val colName = c.name
      if (c.dataType.simpleString.indexOf("array") == 0) {
        regexp_replace(
          regexp_replace(col(colName).cast("string"), lit("^."), lit("{")),
          lit(".$"),
          lit("}")
        ).as(colName)
      } else if (c.dataType.simpleString.indexOf("string") == 0) {
        regexp_replace(
          regexp_replace(col(colName), lit("\\u0000"), lit("")),
          lit("\r\n|\r"),
          lit("\n")
        ).as(colName)
      } else {
        col(colName)
      }
    })
    df.select(newCols: _*)
  }

  def dataframeMapsAndStructsToJson(
      df: Dataset[Row]
  ): DataFrame = {
    val newCols = df.schema.fields.map(c => {
      val colName = c.name
      if (c.dataType.isInstanceOf[MapType] || c.dataType
            .isInstanceOf[StructType]) {
        to_json(col(colName)).as(colName)
      } else {
        col(colName)
      }
    })
    df.select(newCols: _*)
  }

  def sqlExecWithResult(
      spark: SparkSession,
      url: String,
      query: String,
      password: String = "",
      params: List[Any] = Nil
  ): Dataset[Row] = {
    val conn = connOpen(url, password)
    try {

      val st: PreparedStatement = conn.prepareStatement(query)
      val rs = parametrize(st, params).executeQuery()

      import scala.collection.mutable.ListBuffer
      val c = new ListBuffer[Row]()
      while (rs.next()) {
        val b = (1 to rs.getMetaData.getColumnCount).map { idx =>
          {
            val res = rs.getMetaData.getColumnClassName(idx) match {
              case "java.lang.String"     => rs.getString(idx)
              case "java.lang.Boolean"    => rs.getBoolean(idx)
              case "java.lang.Long"       => rs.getLong(idx)
              case "java.lang.Integer"    => rs.getInt(idx)
              case "java.math.BigDecimal" => rs.getDouble(idx)
              case "java.sql.Date"        => rs.getDate(idx)
              case "java.sql.Timestamp"   => rs.getTimestamp(idx)
              case _                      => rs.getString(idx)
            }
            if (rs.wasNull()) null // test wether the value was null
            else res
          }
        }
        c += Row.fromSeq(b)
      }
      val b = spark.sparkContext.makeRDD(c)
      val schema = jdbcMetadataToStructType(rs.getMetaData)

      spark.createDataFrame(b, schema)
    } catch {
      case e: Exception => {
        logger.error(e.getMessage)
        throw e
      }
    } finally {
      conn.close()
    }
  }

  def parametrize(st: PreparedStatement, params: List[Any]): PreparedStatement = {
    for ((obj, i) <- params.zipWithIndex) {
      obj match {
        case s: String       => st.setString(i + 1, s)
        case Some(s: String) => st.setString(i + 1, s)
        case s: Option[String] if s.isEmpty =>
          st.setNull(i + 1, java.sql.Types.VARCHAR)
        case b: Boolean       => st.setBoolean(i + 1, b)
        case Some(b: Boolean) => st.setBoolean(i + 1, b)
        case s: Option[Boolean] if s.isEmpty =>
          st.setNull(i + 1, java.sql.Types.BOOLEAN)
        case l: Long       => st.setLong(i + 1, l)
        case Some(l: Long) => st.setLong(i + 1, l)
        case s: Option[Long] if s.isEmpty =>
          st.setNull(i + 1, java.sql.Types.BIGINT)
        case i: Integer       => st.setInt(i + 1, i)
        case Some(i: Integer) => st.setInt(i + 1, i)
        case s: Option[Integer] if s.isEmpty =>
          st.setNull(i + 1, java.sql.Types.INTEGER)
        case b: java.math.BigDecimal => st.setDouble(i + 1, b.doubleValue())
        case Some(b: java.math.BigDecimal) =>
          st.setDouble(i + 1, b.doubleValue())
        case s: Option[java.math.BigDecimal] if s.isEmpty =>
          st.setNull(i + 1, java.sql.Types.DOUBLE)
        case d: java.sql.Date       => st.setDate(i + 1, d)
        case Some(d: java.sql.Date) => st.setDate(i + 1, d)
        case s: Option[java.sql.Date] if s.isEmpty =>
          st.setNull(i + 1, java.sql.Types.DATE)
        case t: Timestamp       => st.setTimestamp(i + 1, t)
        case Some(t: Timestamp) => st.setTimestamp(i + 1, t)
        case s: Option[Timestamp] if s.isEmpty =>
          st.setNull(i + 1, java.sql.Types.TIMESTAMP)
        case _ =>
          throw new UnsupportedEncodingException(
            obj.getClass.getCanonicalName + " type not yet supported for prepared statements"
          )
      }
    }
    st
  }

  def jdbcMetadataToStructType(meta: ResultSetMetaData): StructType = {
    StructType((1 to meta.getColumnCount).map { idx =>
      meta.getColumnClassName(idx) match {
        case "java.lang.String" =>
          StructField(meta.getColumnLabel(idx), StringType)
        case "java.lang.Boolean" =>
          StructField(meta.getColumnLabel(idx), BooleanType)
        case "java.lang.Integer" =>
          StructField(meta.getColumnLabel(idx), IntegerType)
        case "java.lang.Long" => StructField(meta.getColumnLabel(idx), LongType)
        case "java.math.BigDecimal" =>
          StructField(meta.getColumnLabel(idx), DoubleType)
        case "java.sql.Date" => StructField(meta.getColumnLabel(idx), DateType)
        case "java.sql.Timestamp" =>
          StructField(meta.getColumnLabel(idx), TimestampType)
        case _ => StructField(meta.getColumnLabel(idx), StringType)
      }
    })
  }

}

class ExactPartitioner(partitions: Int) extends Partitioner {

  def getPartition(key: Any): Int = {
    key match {
      case l: Long => math.abs(l.toInt % numPartitions())
      case i: Int  => math.abs(i % numPartitions())
      case _       => math.abs(key.hashCode() % numPartitions())
    }
  }

  def numPartitions(): Int = partitions
}
