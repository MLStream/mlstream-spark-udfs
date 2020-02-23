package mlstream.udf.topk.spark
import mlstream.testutils.SparkTestUtils
import mlstream.testutils.{ SparkStreamingTestUtils, TestUtils }

import org.scalatest._
import prop._

class SparkTopKTableTest
    extends PropSpec
    with TableDrivenPropertyChecks
    with Matchers {
  val examples =
    Table(
      "type",
      new SparkTestUtils.IntGenType(),
      new SparkTestUtils.StringGenType(),
      new SparkTestUtils.StructGenType())

  property("each dataset type should work") {
    forAll(examples) { t =>
      SparkTestUtils.withSpark(sparkSession => {
        SparkTopK.registerUDFs(sparkSession)
        // TODO: be able to choose between genererateDataset and generateLargeDataset
        val ds = SparkTestUtils.generateDataset(sparkSession, t)
        ds.createOrReplaceTempView("test_table")

        val prepared = sparkSession.sql(
          """
            | SELECT extract_approx_topk(distribution)
            | FROM
            | (SELECT approx_topk(key, value, 100, "100MB") AS distribution from test_table) tmp
        """.stripMargin)

        // TODO: test with windows test
        /*
        val prepared_ =
          sparkSession.sql("""
            | SELECT extract_approx_topk(distribution)
            | FROM
            | (SELECT key,
            |   approx_topk(key, value, 10, "100MB")
            |   OVER (PARTITION BY key) as distribution
            |  from test_table)
        """.stripMargin)
         */

        TestUtils
          .timed("approx_topk", {
            prepared.show()
          })
          .result
        val result = prepared.collect()
        /*
        We get a table like
        +---+-----+-----+
        |key|value|error|
        +---+-----+-----+
        |100|  5.0|  0.0|
        | 99|  4.0|  0.0|
        | 98|  3.0|  0.0|
        | 97|  2.0|  0.0|
        | 96|  1.0|  0.0|
        +---+-----+-----+
         */
        val resultMap = t.toMap(result)
        resultMap should be(t.exampleData())
      })
    }
  }

  property("streaming type") {
    forAll(examples) { t =>
      SparkStreamingTestUtils.streamingTest(t)
    }
  }
}
