package mlstream.udf.topk.impl

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataInputStream,
  DataOutputStream
}

import mlstream.utils.genericsort.{ GenericSorter, Interface }

/**
 * An implementation of TopK for numeric types
 */
trait AbstractKeyOps[@specialized(Int, Long) T] {
  def newKeyArray(n: Int): Array[T]
  def getSortKeyIface(
      @specialized(Int, Long) keys: Array[T],
      values: Array[Float],
      ptr: Int): Interface
  def getValueSortIface(
      @specialized(Int, Long) values: Array[Float],
      keys: Array[T],
      ptr: Int): Interface
  def writeKeys(
      dataStream: DataOutputStream,
      @specialized(Int, Long) keys: Array[T],
      ptr: Int): Unit
  def readKeys(dataStream: DataInputStream, len: Int, ptr: Int): Array[T]
}

final case object IntKeyOps extends AbstractKeyOps[Int] {
  def newKeyArray(len: Int): Array[Int] = {
    Array.ofDim[Int](len)
  }

  def getSortKeyIface(
      keys: Array[Int],
      values: Array[Float],
      ptr: Int): Interface = {
    new AscIntFloatArrayInterface(keys, values, ptr)
  }

  def getValueSortIface(
      values: Array[Float],
      keys: Array[Int],
      ptr: Int): Interface = {
    new DescFloatKeyArrayInterfaceDesc[Int](values, keys, ptr)
  }

  def writeKeys(
      dataStream: DataOutputStream,
      keys: Array[Int],
      ptr: Int): Unit = {
    var i = 0
    while (i < ptr) {
      dataStream.writeInt(keys(i))
      i += 1
    }
  }

  def readKeys(dataStream: DataInputStream, len: Int, ptr: Int): Array[Int] = {
    val keys: Array[Int] = Array.ofDim[Int](len)
    var i = 0
    while (i < ptr) {
      keys(i) = dataStream.readInt()
      i += 1
    }
    keys
  }
}

final case object LongKeyOps extends AbstractKeyOps[Long] {
  def newKeyArray(len: Int): Array[Long] = {
    Array.ofDim[Long](len)
  }

  def getSortKeyIface(
      keys: Array[Long],
      values: Array[Float],
      ptr: Int): Interface = {
    new AscLongFloatArrayInterface(keys, values, ptr)
  }

  def getValueSortIface(
      values: Array[Float],
      keys: Array[Long],
      ptr: Int): Interface = {
    new DescFloatKeyArrayInterfaceDesc[Long](values, keys, ptr)
  }

  def writeKeys(
      dataStream: DataOutputStream,
      keys: Array[Long],
      ptr: Int): Unit = {
    var i = 0
    while (i < ptr) {
      dataStream.writeLong(keys(i))
      i += 1
    }
  }

  def readKeys(dataStream: DataInputStream, len: Int, ptr: Int): Array[Long] = {
    val keys: Array[Long] = Array.ofDim[Long](len)
    var i = 0
    while (i < ptr) {
      keys(i) = dataStream.readLong()
      i += 1
    }
    keys
  }
}

final class NumericTopKImpl[@specialized(Int, Long) FixedKey](
    val suggestedMaxSize: Int,
    @specialized(Int, Long) keyOps: AbstractKeyOps[FixedKey],
    @specialized(Int, Long) var keys: Array[FixedKey],
    var values: Array[Float],
    var ptr: Int,
    var totalCnt: Float,
    var numCompactions: Float)
    extends AbstractTopKImpl[NumericTopKImpl[FixedKey], FixedKey, FixedKey] {

  val maxSize = BinaryUtils.adjustToPowerOf2(suggestedMaxSize)

  def this(
      maxSize: Int,
      @specialized(Int, Long) keyOps: AbstractKeyOps[FixedKey]) {
    this(
      maxSize,
      keyOps,
      keyOps.newKeyArray(0),
      Array.ofDim[Float](0),
      0,
      0.0f,
      0.0f)
  }

  /**
   * Aggregates keys and values by removing duplicate keys and adding values
   * for each key
   */
  def deduplicate(): Unit = {
    if (ptr > 1) {
      val sorter = new GenericSorter(keyOps.getSortKeyIface(keys, values, ptr))
      sorter.Sort()

      var i = 1
      var j = 0
      while (i < ptr) {
        assert(j < i)
        if (keys(j) == keys(i)) {
          values(j) += values(i)
        } else {
          j += 1
          keys(j) = keys(i)
          values(j) = values(i)
        }
        i += 1
      }
      ptr = j + 1
    }
  }

  /**
   *  Puts k-th largest element at the right position and ensures that
   *  elements at smaller position are larger or equal, and that elements
   *  at larger positions are smaller
   */
  def partByValue(k: Int): Unit = {
    assert(ptr <= keys.length)
    assert(k < ptr)
    val sorter = new GenericSorter(keyOps.getValueSortIface(values, keys, ptr))
    sorter.Partition(k)
  }

  def sortByValue(): Unit = {
    val sorter = new GenericSorter(
      new DescFloatKeyArrayInterfaceDesc(values, keys, ptr))
    sorter.Sort()
  }

  /**
   * Keeps only the items with largest k values
   */
  def keepLargestK(k: Int): Unit = {
    if (k < ptr) {
      partByValue(k)
      val threshold = values(k)
      var i = 0
      while (i < k && values(i) > threshold) {
        values(i) -= threshold
        i += 1
      }
      numCompactions += threshold
      ptr = i
    }
  }

  /**
   * Gets the summary of the top k elements
   */
  def getSummary(k: Int, converter: FixedKey => FixedKey)
      : (Array[FixedKey], Array[Float], Float, Float) = {
    deduplicate()
    sortByValue()
    val adjustedK = Math.min(k, ptr)
    (keys.take(adjustedK), values.take(adjustedK), totalCnt, numCompactions)
  }

  /**
   * Doubles the storage but upto maxSize
   */
  def grow(): Unit = {
    val currentKeys = keys
    val currentValues = values
    val newSize = Math.min(maxSize, Math.max(1, 2 * currentKeys.length))

    keys = keyOps.newKeyArray(newSize)
    values = Array.ofDim[Float](newSize)

    val copyLen = Math.min(ptr, currentKeys.length)
    System.arraycopy(currentKeys, 0, keys, 0, copyLen)
    System.arraycopy(currentValues, 0, values, 0, copyLen)
  }

  /**
   * Merges other summary into the current summary
   */
  final override def merge(
      @specialized(Int, Long) other: NumericTopKImpl[FixedKey])
      : NumericTopKImpl[FixedKey] = {
    other.deduplicate()
    other.sortByValue()

    // note: insertKeyValue updates the total count, so we save it
    val saveTotalCnt = totalCnt

    var i = 0
    while (i < other.ptr) {
      insertKeyValue(other.keys(i), other.values(i))
      i += 1
    }

    totalCnt = saveTotalCnt + other.totalCnt
    numCompactions += other.numCompactions
    this
  }

  final override def insertKeyValue(
      @specialized(Int, Long) key: FixedKey,
      value: Float): Unit = {
    if (ptr >= keys.length) {
      deduplicate()
      // If deduplication did not decrease the space enough (at least half of the space
      // should be empty)
      if (ptr >= keys.length / 2) {
        // if we can still grow
        if (keys.length < maxSize) {
          grow()
          assert(keys.length <= maxSize)
        } else {
          // We at maximum possible size, so the only way
          // forward is to drop elements with the lowest count
          assert(keys.length == maxSize)
          keepLargestK(maxSize / 2)
        }
      }
      // When we do an O(n) operation, we want to have empty space of n/2
      assert(2 * ptr <= keys.length)
    }
    assert(ptr < keys.length)
    assert(keys.length == values.length)
    keys(ptr) = key
    values(ptr) = value
    ptr += 1

    totalCnt += value
  }

  def serialize(): Array[Byte] = {
    deduplicate()
    val byteStream = new ByteArrayOutputStream()
    val dataStream = new DataOutputStream(byteStream)
    dataStream.writeInt(maxSize)
    dataStream.writeInt(keys.length)
    dataStream.writeInt(ptr)
    dataStream.writeFloat(totalCnt)
    dataStream.writeFloat(numCompactions)

    keyOps.writeKeys(dataStream, keys, ptr)

    var i = 0
    while (i < ptr) {
      dataStream.writeFloat(values(i))
      i += 1
    }
    byteStream.toByteArray
  }

}

case class NumericTopKImplFactory[@specialized(Int, Long) FixedKey](
    @specialized(Int, Long) keyOps: AbstractKeyOps[FixedKey])
    extends AbstractTopKImplFactory[NumericTopKImpl[FixedKey]] {
  def deserialize(storageFormat: Array[Byte]): NumericTopKImpl[FixedKey] = {
    val byteStream = new ByteArrayInputStream(storageFormat)
    val dataStream = new DataInputStream(byteStream)

    val maxSize = dataStream.readInt()
    val len = dataStream.readInt()
    val ptr = dataStream.readInt()
    assert(ptr <= len)
    val totalCnt = dataStream.readFloat()
    val numCompactions = dataStream.readFloat()

    val values: Array[Float] = Array.ofDim[Float](len)

    val keys = keyOps.readKeys(dataStream, len, ptr)

    var i = 0
    while (i < ptr) {
      values(i) = dataStream.readFloat()
      i += 1
    }
    new NumericTopKImpl[FixedKey](
      maxSize,
      keyOps,
      keys,
      values,
      ptr,
      totalCnt,
      numCompactions)
  }

  def newInstance(maxSize: Int): NumericTopKImpl[FixedKey] = {
    new NumericTopKImpl[FixedKey](maxSize, keyOps)
  }
}
