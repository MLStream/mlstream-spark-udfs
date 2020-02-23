package mlstream.udf.topk

import mlstream.testutils.SparkTestUtils
import mlstream.udf.topk.spark.SparkTopK
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{
  DataType,
  FloatType,
  IntegerType,
  LongType,
  StructField,
  StructType
}
import org.scalatest.PropSpec
import org.scalatest._
import prop._

class SparkTopKInvalidParametersTest
    extends PropSpec
    with TableDrivenPropertyChecks {

  def registerInvalidDataFrame(
      sparkSession: SparkSession,
      keyType: DataType): Unit = {

    val data = keyType match {
      case IntegerType => List(Row(1, 1.0f), Row(2, 1.0f))
      case LongType    => List(Row(1L, 1.0f), Row(2L, 1.0f))
      case FloatType   => List(Row(1.0f, 1.0f), Row(2.0f, 1.0f))
    }

    val input: RDD[Row] = sparkSession.sparkContext.parallelize(data, 4)

    val schema = StructType(
      Seq(StructField("key", keyType), StructField("value", FloatType)))
    val df = sparkSession.createDataFrame(input, schema)
    df.createOrReplaceTempView("test_table")
  }

  trait TestOutcome {
    def validate(block: => Unit): Unit
  }

  case class FailsWithMessage(msg: String) extends TestOutcome {
    def validate(block: => Unit): Unit = {
      try {
        block
        assert(false, "Expecting AnalysisException, not success")
      } catch {
        case e: org.apache.spark.sql.AnalysisException => {
          val errMsg = e.getMessage()
          println(errMsg)
          val pos = errMsg.indexOf(msg)
          assert(pos >= 0, errMsg)
        }
        case _: Throwable => {
          assert(false, "Expecting AnalysisException, not any other exception")
        }
      }
    }
  }
  case class Succeeds() extends TestOutcome {
    def validate(block: => Unit): Unit = {
      block
      assert(true)
    }
  }

  case class TestCase(keyType: DataType, query: String, outcome: TestOutcome)

  val testCases_ =
    Table(
      "type",
      TestCase(
        FloatType,
        "SELECT approx_topk() AS distribution from test_table",
        FailsWithMessage("Expression at position 1 is must be provided")),
      TestCase(
        FloatType,
        "SELECT approx_topk(key) AS distribution from test_table",
        FailsWithMessage("Key type can only be int, long, string or struct")),
      TestCase(
        IntegerType,
        "SELECT approx_topk(key) AS distribution from test_table",
        Succeeds()),
      TestCase(
        LongType,
        "SELECT approx_topk(key) AS distribution from test_table",
        Succeeds()),
      TestCase(
        LongType,
        "SELECT extract_approx_topk(distribution) FROM (SELECT approx_topk(key, CAST(1.0 AS FLOAT)) AS distribution from test_table)",
        Succeeds()),
      TestCase(
        IntegerType,
        "SELECT approx_topk(key, CAST(1.0 AS FLOAT)) AS distribution from test_table",
        Succeeds()),
      TestCase(
        IntegerType,
        "SELECT approx_topk(key, CAST(1.0 as FLOAT), 100, '10MB') AS distribution from test_table",
        Succeeds()))

  val testCases =
    Table(
      "type",
      TestCase(
        LongType,
        "SELECT extract_approx_topk(distribution) FROM (SELECT approx_topk(key, CAST(1.0 AS FLOAT)) AS distribution from test_table)",
        Succeeds()))

  property("test failure cases") {
    forAll(testCases) { t =>
      SparkTestUtils.withSpark(sparkSession => {
        SparkTopK.registerUDFs(sparkSession)
        registerInvalidDataFrame(sparkSession, t.keyType)
        t.outcome.validate({
          sparkSession.sql(t.query).show()
        })
      })
    }
  }

}
