package mlstream.udf.topk

import mlstream.udf.topk.impl.{ IntKeyOps, NumericTopKImpl }
import org.scalatest.FunSpec

class NumericTopKImplPerFuncTest extends FunSpec {

  def deduplication(
      keys: Array[Int],
      values: Array[Float]): (Array[Int], Array[Float]) = {
    val topK = new NumericTopKImpl(100, IntKeyOps)
    topK.keys = keys
    topK.values = values
    topK.ptr = topK.keys.length
    topK.deduplicate()
    (topK.keys.take(topK.ptr), topK.values.take(topK.ptr))
  }

  it("deduplication_test") {
    val table: List[((Array[Int], Array[Float]), (Array[Int], Array[Float]))] =
      List(
        ((Array.empty, Array.empty), (Array.empty, Array.empty)),
        (
          (Array.range(0, 1), Array.range(0, 1).map(_.toFloat)),
          (Array.range(0, 1), Array.range(0, 1).map(_.toFloat))),
        (
          (Array.range(0, 2), Array.range(0, 2).map(_.toFloat)),
          (Array.range(0, 2), Array.range(0, 2).map(_.toFloat))),
        (
          (Array(1, 1, 1, 1), Array(1.0f, 1.0f, 1.0f, 1.0f)),
          (Array(1), Array(4.0f))),
        (
          (Array(1, 1, 2, 2, 2), Array(1.0f, 1.0f, 1.0f, 1.0f, 1.0f)),
          (Array(1, 2), Array(2.0f, 3.0f))))

    for ((input, expected) <- table) {
      val (keys, values) = input
      val (outputKeys, outputValues) = deduplication(keys, values)
      val (expectedKeys, expectedValues) = expected
      assertResult(expectedKeys)(outputKeys)
      assertResult(expectedValues)(outputValues)
    }
  }

}
