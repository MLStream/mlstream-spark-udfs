package mlstream.testutils

import java.io.DataOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.scalatest.FunSpec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.window
import scala.concurrent.{ Await, Future }
import java.net.{ InetAddress, ServerSocket }
import mlstream.udf.topk.spark.SparkTopK
import scala.concurrent.duration._
import org.apache.log4j.{ Level, Logger }

object SparkStreamingTestUtils extends FunSpec {
  val logger = Logger.getLogger(SparkStreamingTestUtils.getClass)
  logger.setLevel(Level.DEBUG)

  case class GeneratorFuncs(generator: () => Unit, cleaner: () => Unit)

  def server(port: Int, count: Int): GeneratorFuncs = {
    val serverSocket = new ServerSocket(port, 0, InetAddress.getLoopbackAddress())
    def generator() {
      // waits to establish connection
      val connectionSocket = serverSocket.accept
      def onClientConnect(): Unit = {
        val client = new DataOutputStream(connectionSocket.getOutputStream)
        var i = 0
        while (i < count) {
          val now = Calendar.getInstance().getTime()
          val minuteFormat = new SimpleDateFormat(SparkTestUtils.timeFormat)
          val timeAsStr = minuteFormat.format(now)
          val line = timeAsStr + "," + i.toString() + "\n"
          client.writeBytes(line)
          if (i % 100000 == 0) {
            Thread.sleep(250)
            logger.debug(s"Sent ${i} items")
          }
          i += 1
        }
        client.close()
        connectionSocket.close()
      }
      new Thread(new Runnable {
        override def run(): Unit = {
          onClientConnect()
        }
      }).start()
    }

    def cleaner(): Unit = {
      serverSocket.close()
    }

    GeneratorFuncs(generator, cleaner)
  }

  def selectFromTable(sess: SparkSession, expectedCnt: Int): Unit = {
    var done = false
    while (!done) {

      if (sess.catalog.tableExists("output_table")) {
        val res =
          sess.sql("SELECT cnt, topk.totalCount, topk FROM output_table")
        val cached = res.cache()
        for (row <- cached.collect()) {
          val cnt = row.getAs[Long](0)
          val totalCnt = row.getAs[Long](0)
          assertResult(cnt)(totalCnt)
          if (cnt == expectedCnt) {
            done = true
          }
        }
        logger.debug(s"Output from in-memory output_table ${cached.count()}")
        if (cached.count() > 0) {
          cached.show(10)
        }
        cached.unpersist(true)
      }
      Thread.sleep(6000)
    }
  }

  def streamingTest(genT: SparkTestUtils.GenType) {
    import scala.concurrent.ExecutionContext.Implicits.global
    val sess = SparkTestUtils.getSparkSession()
    val cnt = 100
    val port = 9999
    val genFuns = server(port, cnt)

    val streamingJob = SparkTestUtils.withSparkStream(sess, port) {
      df: DataFrame =>
        {
          SparkTopK.registerUDFs(sess)

          val topK = SparkTopK
            .approx_topk(col("value"), lit(1.0f), 10, "10MB")
            .alias("topk")

          df.withWatermark("ts", "60 seconds")
            .selectExpr("ts", genT.convertExpr) // value converted to a tested type, e.g. int, string, struct
            .groupBy(window(col("ts"), "60 seconds", "30 seconds"))
            .agg(count(col("value")).alias("cnt"), topK) //windowedTopK
        }
    }

    // must be called first
    val futDataGen = Future {
      genFuns.generator()
    }
    // wait so that accept is called
    Thread.sleep(2000)

    // next start the streaming job
    val futStreamingJob = Future {
      streamingJob.run()
    }
    Thread.sleep(2000)

    // after streaming job is running and consuming
    // we can start checking for expected output in an
    // in-memory table
    val futVerifyResults = Future {
      selectFromTable(sess, cnt)
      // close after verifier reports as done
      streamingJob.stop()
      genFuns.cleaner()
    }

    try {
      Await.result(
        Future.sequence(Seq(futDataGen, futStreamingJob, futVerifyResults)),
        180.seconds)
    } catch {
      case _: Throwable => {
        assert(false)
      }
    } finally {
      sess.stop()
    }
  }
}
