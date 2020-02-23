package mlstream.udf.topk

import mlstream.udf.topk.impl.{
  AbstractTopKImpl,
  ByteArrayTopKImpl,
  IntKeyOps,
  LongKeyOps,
  NumericTopKImpl
}
import org.scalatest.FunSpec

class NumericTopKImplTest extends FunSpec {
  trait BaseCase {
    def topKImpl(maxSize: Int): AbstractTopKImpl[_, _, _]
    def insertAll(topK: AbstractTopKImpl[_, _, _], keys: Seq[Int]): Any
    def getSummary(
        topK: AbstractTopKImpl[_, _, _],
        numItems: Int): (Array[Int], Array[Float], Float, Float)
  }

  case class IntCase() extends BaseCase {
    def topKImpl(maxSize: Int): AbstractTopKImpl[_, _, _] = {
      new NumericTopKImpl(maxSize, IntKeyOps)
    }

    def insertAll(topK: AbstractTopKImpl[_, _, _], keys: Seq[Int]): Any = {
      val castTopK = topK.asInstanceOf[NumericTopKImpl[Int]]
      for (key <- keys) {
        castTopK.insertKeyValue(key, 1.0f)
      }
    }

    def getSummary(
        topK: AbstractTopKImpl[_, _, _],
        numItems: Int): (Array[Int], Array[Float], Float, Float) = {
      val castTopK = topK.asInstanceOf[NumericTopKImpl[Int]]
      val (keys, values, totalCnt, numCompactions) =
        castTopK.getSummary(numItems, x => x)
      (keys, values, totalCnt, numCompactions)
    }

  }

  case class LongCase() extends BaseCase {
    def topKImpl(maxSize: Int): AbstractTopKImpl[_, _, _] = {
      new NumericTopKImpl(maxSize, LongKeyOps)
    }

    def insertAll(topK: AbstractTopKImpl[_, _, _], keys: Seq[Int]): Any = {
      val castTopK = topK.asInstanceOf[NumericTopKImpl[Long]]
      for (key <- keys) {
        castTopK.insertKeyValue(key.toLong, 1.0f)
      }
    }

    def getSummary(
        topK: AbstractTopKImpl[_, _, _],
        numItems: Int): (Array[Int], Array[Float], Float, Float) = {
      val castTopK = topK.asInstanceOf[NumericTopKImpl[Long]]
      val (keys, values, totalCnt, numCompactions) =
        castTopK.getSummary(numItems, x => x)
      (keys.map(_.toInt), values, totalCnt, numCompactions)
    }
  }

  case class StringCase() extends BaseCase {
    type T = ByteArrayTopKImpl
    def topKImpl(maxSize: Int): AbstractTopKImpl[_, _, _] = {
      new T(maxSize)
    }

    def insertAll(topK: AbstractTopKImpl[_, _, _], keys: Seq[Int]): Any = {
      val castTopK = topK.asInstanceOf[T]
      for (key <- keys) {
        castTopK.insertKeyValue(key.toString.getBytes, 1.0f)
      }
    }

    def getSummary(
        topK: AbstractTopKImpl[_, _, _],
        numItems: Int): (Array[Int], Array[Float], Float, Float) = {
      val castTopK = topK.asInstanceOf[ByteArrayTopKImpl]
      val (keys, values, totalCnt, numCompactions) =
        castTopK.getSummary(numItems, x => x)
      (
        keys.map(x => new String(x.asInstanceOf[Array[Byte]]).toInt),
        values,
        totalCnt,
        numCompactions)
    }
  }

  val ex1: Seq[Int] = (1 to 100000) ++ Seq.iterate(0, 1000)(_ => 0)
  val ex2: Seq[Int] = Seq.iterate(0, 1000)(_ => 0) ++ (1 to 1000)
  val examples: Seq[(Seq[Int], Map[Int, Float])] =
    Seq(
      (Seq(), Map()),
      (Seq(1), Map(1 -> 1)),
      (Seq(1, 1, 1), Map(1 -> 3)),
      (Seq(1, 1, 1, 2, 2), Map(1 -> 3, 2 -> 2)),
      // (ex1,  Map(0 -> 1000)), // if the item is added at the end the count is accurate
      (ex2, Map(0 -> 1000)))

  it("topk basic test") {

    def insertAll[K](topK: AbstractTopKImpl[_, K, _], keys: Seq[K]): Unit = {
      for (key <- keys) {
        topK.insertKeyValue(key, 1.0f)
      }
    }
    val cases = Seq(StringCase(), IntCase(), LongCase())
    for (case_ <- cases) {
      for (maxSize <- Seq(4, 8, 16, 32, 1024)) {
        for ((input, expected) <- examples) {
          val topK = case_.topKImpl(maxSize)
          case_.insertAll(topK, input)
          val (keys, values, totalCnt, numCompactions) =
            case_.getSummary(topK, 1024)
          val actual = keys.zip(values).toMap
          assert(totalCnt == input.length)
          for ((k, v) <- expected) {
            assert(actual.contains(k))
            assert(numCompactions + actual(k) == v)
          }
        }
      }
    }
  }

  it("test dedup") {
    val examples: Seq[(Array[Int], (Array[Int], Array[Float]))] = Seq(
      (Array(), (Array(), Array())),
      (Array(1), (Array(1), Array(1f))),
      (Array(1, 2), (Array(1, 2), Array(1f, 1f))),
      (Array(1, 1), (Array(1), Array(2f))),
      (Array(1, 2, 2), (Array(1, 2), Array(1f, 2f))))

    for (example <- examples) {
      val (
        input: Array[Int],
        (epxectedKeys: Array[Int], expectedValues: Array[Float])) = example
      val topK = new NumericTopKImpl(100, IntKeyOps)
      topK.keys = input
      topK.values = input.map(_ => 1.0f)
      topK.ptr = input.length
      topK.deduplicate()
      val actualKeys = topK.keys.take(topK.ptr)
      val actualValues = topK.values.take(topK.ptr)
      assertResult(epxectedKeys) { actualKeys }
      assertResult(expectedValues) { actualValues }
    }
  }
}
