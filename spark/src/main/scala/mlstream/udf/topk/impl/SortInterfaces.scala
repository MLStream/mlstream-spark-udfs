package mlstream.udf.topk.impl

import mlstream.utils.genericsort.Interface

sealed trait KeyComparator[@specialized(Int, Long) K] {
  def less(@specialized(Int, Long) a: Array[K], i: Int, j: Int): Boolean
}

final object IntComparator extends KeyComparator[Int] {
  @inline final override def less(a: Array[Int], i: Int, j: Int): Boolean = {
    a(i) < a(j)
  }
}

final object LongComparator extends KeyComparator[Long] {
  @inline final override def less(a: Array[Long], i: Int, j: Int): Boolean = {
    a(i) < a(j)
  }
}

class AscKeyFloatArrayInterface[@specialized(Int, Long) K](
    @specialized(Int, Long) keyComprator: KeyComparator[K],
    @specialized(Int, Long) a: Array[K],
    b: Array[Float],
    limit: Int)
    extends Interface {
  assert(limit >= 0 && limit <= a.length)

  @inline final override def Len(): Int = {
    limit
  }
  // Less reports whether the element with
  // index i should sort before the element with index j.
  @inline final override def Less(i: Int, j: Int): Boolean = {
    keyComprator.less(a, i, j)
  }
  // Swap swaps the elements with indexes i and j.
  final override def Swap(i: Int, j: Int): Unit = {
    val tmp = a(i)
    a(i) = a(j)
    a(j) = tmp

    val tmpF = b(i)
    b(i) = b(j)
    b(j) = tmpF
  }
}

final class DescFloatKeyArrayInterfaceDesc[@specialized(Int, Long) K](
    a: Array[Float],
    @specialized(Int, Long) b: Array[K],
    limit: Int)
    extends Interface {
  assert(limit >= 0 && limit <= a.length)
  @inline final override def Len(): Int = {
    limit
  }
  // Less reports whether the element with
  // index i should sort before the element with index j.
  @inline final override def Less(i: Int, j: Int): Boolean = {
    a(i) > a(j) // note: sort is in descending
  }

  @inline final def swapA(i: Int, j: Int): Unit = {
    val tmp = a(i)
    a(i) = a(j)
    a(j) = tmp
  }

  @inline final def swapB(i: Int, j: Int): Unit = {
    val tmpF = b(i)
    b(i) = b(j)
    b(j) = tmpF
  }

  // Swap swaps the elements with indexes i and j.
  final override def Swap(i: Int, j: Int): Unit = {
    swapA(i, j)
    swapB(i, j)
  }
}

final class AscIntFloatArrayInterface(
    a: Array[Int],
    b: Array[Float],
    limit: Int)
    extends AscKeyFloatArrayInterface[Int](IntComparator, a, b, limit) {}

final class AscLongFloatArrayInterface(
    a: Array[Long],
    b: Array[Float],
    limit: Int)
    extends AscKeyFloatArrayInterface[Long](LongComparator, a, b, limit) {}
