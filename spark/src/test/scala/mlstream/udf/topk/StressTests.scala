package mlstream.udf.topk

import org.apache.spark.sql.catalyst.expressions.XXH64
import org.scalatest.FunSpec
import java.nio.ByteBuffer

import mlstream.testutils.TestUtils
import mlstream.udf.topk.impl.{ ByteArrayTopKImpl, IntKeyOps, NumericTopKImpl }
import org.scalatest.tags.Slow

@Slow
class StressTests extends FunSpec {
  it("insert stress test") {
    def f(): Unit = {
      val maxItems = 10000000
      val topK = new NumericTopKImpl(maxItems, IntKeyOps)
      var i = 0L
      val testSize = 100000000L

      while (i < testSize) {
        var j = XXH64.hashLong(i, 0XC2B2AE3D27D4EB4FL).toInt
        // map half of the numbers to the same number,
        // the other half leave random
        // interestingly, if the range of j is very small e.g. its only one
        // of say 10 possibilities, the algorithm is much faster
        // also slow if values are random floats
        if ((j & 1) == 0) {
          j = 123
        }
        topK.insertKeyValue(j, 1.0f)
        i += 1
      }
    }
    val result = TestUtils.timed("insert stress test", f)
    assert(result.timeMillisecs < 30 * 1000)
  }

  it("insert stress test strings") {
    def f(): Unit = {
      val maxItems = 10000000
      val topK = new ByteArrayTopKImpl(maxItems)
      var i = 0L
      val testSize = 100000000L
      val rnd = new scala.util.Random(3316)

      while (i < testSize) {
        var j = XXH64.hashLong(i, 0XC2B2AE3D27D4EB4FL).toInt
        // run-time complexity depends on the entropy of the input sequence
        if ((j & 1) == 0) {
          j = 123
        }
        val bytes = ByteBuffer.allocate(4).putInt(j).array
        topK.insertKeyValue(bytes, rnd.nextFloat())
        i += 1
      }
    }
    val result = TestUtils.timed("insert stress test", f)
    assert(result.timeMillisecs < 45 * 1000)
  }

  it("adding 1 billion longs using a float results in big error") {
    var i = 0L
    val limit = 1000000000L
    var counter: Float = 0.0f
    while (i < limit) {
      counter += 1.0f
      i += 1
    }
    println(counter)
    val e = 100.0 * (counter - limit.toFloat) / (limit.toFloat)
    println(s"Error ${e}%")
  }
}
