package mlstream.utils.genericsort

import org.scalatest.FunSpec

final class IntArrayWrapper(a: Array[Int]) extends Interface {
  def Len(): Int = {
    a.length
  }
  // Less reports whether the element with
  // index i should sort before the element with index j.
  def Less(i: Int, j: Int): Boolean = {
    a(i) < a(j)
  }
  // Swap swaps the elements with indexes i and j.
  def Swap(i: Int, j: Int): Unit = {
    val tmp = a(i)
    a(i) = a(j)
    a(j) = tmp
  }
}

object ArrayWrapper {
  def wrapIntArray(a: Array[Int]): IntArrayWrapper = {
    new IntArrayWrapper(a)
  }
}

class GenericSortTest extends FunSpec {
  it("basic checks") {
    val rnd = new scala.util.Random(98484848)
    val a1 = (1 to 10000).map(_ => rnd.nextInt()).toArray
    def dup(x: Int): Array[Int] = {
      Array(x, x)
    }
    val a2 = (1 to 10000).flatMap(dup(_)).toArray
    val examples: Seq[Array[Int]] = Seq(
      Array(),
      Array(1),
      Array(1, 2),
      Array(2, 1),
      (1 to 10000).toArray,
      (1 to 10000).reverse.toArray,
      a1,
      a1 ++ a1,
      a2,
      a2 ++ a2)

    for (example <- examples) {
      val mut: Array[Int] = example.clone
      val sorter = new GenericSorter(ArrayWrapper.wrapIntArray(mut))
      sorter.Sort()
      assertResult(example.sorted) { mut }
    }
  }

  it("median partitioning checks") {
    val rnd = new scala.util.Random(98484848)
    val numRandom = 10000
    val numEqual = 2
    val randomItems = (1 to numRandom).map(_ => rnd.nextInt()).toArray
    val sortedArr = randomItems.sorted
    val plantedMidValue = sortedArr(randomItems.length / 2)
    val a = randomItems ++ (1 to numEqual).map(_ => plantedMidValue)

    val mut: Array[Int] = a.clone
    val p = new GenericSorter(ArrayWrapper.wrapIntArray(mut))
    val mid = a.length / 2
    p.Partition(mid)
    val midValue = mut(mid)
    for (i <- 0 until mid) {
      assert(mut(i) <= midValue)
    }
    for (j <- mid until mut.length) {
      assert(mut(j) >= midValue)
    }
    val eqStart = mut.indexOf(midValue)
    val eqEnd = mut.lastIndexOf(midValue)
    assert(eqEnd - eqStart >= numEqual)
    assert(midValue == plantedMidValue)
    for (i <- eqStart to eqEnd) {
      assert(mut(i) == midValue)
    }
  }

}
