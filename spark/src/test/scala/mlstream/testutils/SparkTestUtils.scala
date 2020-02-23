package mlstream.testutils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{
  FloatType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }

object SparkTestUtils {
  type WithSession = SparkSession => Unit
  type WithStreamDF = (DataFrame) => DataFrame

  val timeFormat = "MM/dd/yyyy HH:mm:ss"

  case class SparkStreamingJob(run: () => Unit, stop: () => Unit)

  def getSparkSession(): SparkSession = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.cores", 4)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")
    sparkSession
  }

  def withSpark(body: WithSession): Unit = {
    val sparkSession = getSparkSession()
    try {
      body(sparkSession)
    } finally {
      sparkSession.stop()
    }
  }

  trait GenType {
    def valueField: StructField = StructField("value", FloatType)
    def dataSchema: StructType
    def exampleData(): Map[Any, Int]
    def fromIntDF(df: DataFrame): DataFrame
    def convertExpr: String
    def toMap(output: Seq[Row]): Map[Any, Int] = {
      val outputAsMap = output.map { item =>
        {
          val key = item.get(item.fieldIndex("key"))
          val value = item.getAs[Float]("value")
          (key, value.toInt)
        }
      }.toMap
      outputAsMap
    }
  }

  class IntGenType extends GenType {
    def dataSchema: StructType = {
      StructType(Seq(StructField("key", IntegerType), valueField))
    }

    def exampleData(): Map[Any, Int] = {
      Map(100 -> 5, 99 -> 4, 98 -> 3, 97 -> 2, 96 -> 1)
    }

    def fromIntDF(df: DataFrame): DataFrame = {
      df.selectExpr("key", "CAST(1.0 AS FLOAT) AS value")
    }

    def convertExpr = {
      "CAST(value AS INTEGER)"
    }
  }

  class StringGenType extends GenType {
    def dataSchema: StructType = {
      StructType(Seq(StructField("key", StringType), valueField))
    }

    def exampleData(): Map[Any, Int] = {
      Map("a" -> 5, "b" -> 4, "c" -> 3, "d" -> 2, "e" -> 1)
    }

    def fromIntDF(df: DataFrame): DataFrame = {
      df.selectExpr("CAST(key AS STRING) AS key", "CAST(1.0 AS FLOAT) AS value")
    }

    def convertExpr: String = {
      "CAST(value AS STRING)"
    }
  }

  class StructGenType extends GenType {
    def dataSchema: StructType = {
      val keySchema = StructType(
        Seq(
          StructField("part1", StringType),
          StructField("part2", IntegerType)))

      StructType(Seq(StructField("key", keySchema), valueField))
    }

    def exampleData(): Map[Any, Int] = {
      Map(
        Row("a", 100) -> 5,
        Row("b", 101) -> 4,
        Row("c", 102) -> 3,
        Row("d", 103) -> 2,
        Row("e", 104) -> 1)
    }

    def fromIntDF(df: DataFrame): DataFrame = {
      df.selectExpr(
        "struct(CAST(key AS STRING) AS part1, key AS part2) AS key",
        "CAST(1.0 AS FLOAT) AS value")
    }

    def convertExpr = {
      "struct(CAST(value AS STRING) AS part1, CAST(value AS INTEGER) AS part2) AS value"
    }
  }

  def generateDataset(sparkSession: SparkSession, g: GenType): DataFrame = {

    val data = g
      .exampleData()
      .flatMap {
        case (key, count) => Seq.range(0, count).map(_ => Row(key, 1.0f))
      }
      .toSeq

    val rdd: RDD[Row] = sparkSession.sparkContext.parallelize(data, 4)
    val schema = g.dataSchema
    sparkSession.createDataFrame(rdd, schema)
  }

  def generateLargeDataset(
      sparkSession: SparkSession,
      g: GenType,
      mult: Int = 10,
      genInRev: Boolean = false): DataFrame = {
    val numSlices = 4
    val df = largeIntsDF(sparkSession, mult, numSlices, genInRev)
    g.fromIntDF(df)
  }

  def largeIntsDF(
      sparkSession: SparkSession,
      mult: Int,
      numSlices: Int = 1,
      genInRev: Boolean = false): DataFrame = {
    // We want to pass a Range to sparkContext.parallelize
    val limit = 10000 * mult
    val highFreqLimit = 10 //400
    val range: Range = if (genInRev) {
      Range.apply(highFreqLimit + limit, 0, -1)
    } else {
      Range.apply(0, highFreqLimit + limit)
    }

    val schema = StructType(
      Seq(StructField("key", IntegerType), StructField("item", IntegerType)))

    val rdd: RDD[Row] = sparkSession.sparkContext
      .parallelize(range, numSlices)
      .map(i => {
        val converted = if (i < highFreqLimit) {
          0
        } else {
          5 + i
        }
        Row(converted % 100, converted)
      })
    sparkSession.createDataFrame(rdd, schema)
  }

  def largeStringDF(
      sparkSession: SparkSession,
      mult: Int,
      numSlices: Int = 1,
      genInRev: Boolean = false): DataFrame = {
    val df = largeIntsDF(sparkSession, mult, numSlices, genInRev: Boolean)
    df.selectExpr("CAST(key AS STRING)", "item")
  }

  def withSparkStream(sess: SparkSession, port: Int)(
      f: WithStreamDF): SparkStreamingJob = {
    val lines = sess.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", port)
      .load()

    import org.apache.spark.sql.functions.to_timestamp
    import org.apache.spark.sql.functions.col
    import sess.implicits._

    def getRow(line: String): Row = {
      val parts = line.split(",")
      Row(parts(0).trim, parts(1).trim)
    }

    val schema = StructType(
      Seq(StructField("ts", StringType), StructField("value", StringType)))

    val encoder = RowEncoder(schema)
    val fmt = "MM/dd/yyyy HH:mm:ss"
    val formatted_lines = lines
      .as[String]
      .map { getRow(_) }(encoder)
      .toDF()
      .select(to_timestamp(col("ts"), fmt).alias("ts"), col("value"))

    formatted_lines.printSchema()
    val agg = f(formatted_lines)

    val query = agg.writeStream
      .outputMode("update")
      .format("memory")
      .queryName("output_table")
      .start()

    SparkStreamingJob(
      () => { query.awaitTermination() },
      () => { query.stop() })
  }
}
