package mlstream.udf.topk.impl

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataInputStream,
  DataOutputStream
}
import java.nio.ByteBuffer
import mlstream.utils.genericsort.GenericSorter
import org.apache.spark.sql.catalyst.expressions.XXH64
import org.apache.spark.unsafe.Platform

object Constants {
  final val lengthSize = 4
  final val valueSize = 4
  final val hashSeed = 48484
}

object AddEntryStatus {
  final val OutOfSpace = -1
}

object InsertKeyValueStatus {
  final val Updated = 1
  final val InProcess = 0
  final val ByteStoreOutOfSpace = -1
  final val HashTableOutOfSpace = -2
}

class ByteStoreNextEntry(var nextPointer: Int = 0) {
  assert(nextPointer >= 0)
  var currentPointer: Int = -1
  var value: Float = 0.0f
  var key: Array[Byte] = null
  var sizeInBytes: Int = 0
}

object BinaryUtils {
  def isPowerOf2(v: Int): Boolean = {
    assert(v >= 2)
    (v & (v - 1)) == 0
  }

  def adjustToNextPowerOf2(v: Int): Int = {
    val adjV = Math.max(2, v)
    val zeros = java.lang.Integer.numberOfLeadingZeros(adjV)
    1 << (32 - zeros)
  }

  def adjustToPowerOf2(v: Int): Int = {
    val adjV = Math.max(2, v)
    if (isPowerOf2(adjV)) {
      adjV
    } else {
      adjustToNextPowerOf2(v)
    }
  }

  /**
   * Aligns the index on a boundary. boundary is a power of 2
   * @param i
   * @param boundary must be a power of 2
   * @return
   */
  def align(i: Int, boundary: Int): Int = {
    val delta = i & (boundary - 1) // returns one of 0, 1, 2, 3
    if (delta == 0) {
      i
    } else {
      i + (boundary - delta)
    }
  }
}

/**
 * ByteArrayStore stores keys and values in a contiguous binary array
 * The format is a contiguous array of
 *  - 4 bytes for length of key,
 *  - 4 bytes for value,
 *  - variable num of bytes for key,
 *  - padding until 4 byte boundary
 *  */
final class ByteArrayStore(
    initialSize: Int,
    var cnt: Int,
    var bytes: Array[Byte],
    var ptr: Int) {

  assert(initialSize >= 16)
  assert(BinaryUtils.isPowerOf2(initialSize))
  private[ByteArrayStore] var bbView = ByteBuffer.wrap(bytes, 0, bytes.length)

  def this(initialSize: Int) {
    this(initialSize, 0, Array.ofDim[Byte](Math.max(16, initialSize)), 0)
  }

  /**
   * Doubles the size of the storage if possible
   */
  def grow(): Boolean = {
    val newSize = Math.max(16L, bytes.length.toLong << 1)
    if (newSize >= Integer.MAX_VALUE) {
      false
    } else {
      val newBytes = Array.ofDim[Byte](newSize.toInt)
      System.arraycopy(bytes, 0, newBytes, 0, ptr)
      bytes = newBytes
      bbView = ByteBuffer.wrap(bytes, 0, bytes.length)
      true
    }
  }

  /**
   * Adds new key/value pair to the array
   */
  def addNewEntry(keyContents: Array[Byte], value: Float): Int = {
    addNewEntry(keyContents, 0, keyContents.length, value)
  }

  /**
   * Adds new entry to top-k structure
   * @param keyBuffer: binary array creating the key contents
   * @param from: offset in the keyBuffer array
   * @param len: length of the key
   * @param value: the value to be added for that key
   * @return
   */
  def addNewEntry(
      keyBuffer: Array[Byte],
      from: Int,
      len: Int,
      value: Float): Int = {
    val requiredSize = len.toLong + Constants.lengthSize + Constants.lengthSize
    if (ptr + requiredSize > bytes.length.toLong) {
      AddEntryStatus.OutOfSpace
    } else {
      // remember the offset we start at
      val start = ptr

      bbView.putInt(ptr, len)
      ptr += Constants.lengthSize
      bbView.putFloat(ptr, value)
      ptr += Constants.valueSize

      System.arraycopy(keyBuffer, from, bytes, ptr, len)
      ptr += len
      ptr = BinaryUtils.align(ptr, 4)

      cnt += 1
      start
    }
  }

  /**
   * Increments the value stored at the external pointer
   * @param atPtr: external pointer
   * @param value: value to be added
   */
  def incrementValueAt(atPtr: Int, value: Float): Unit = {
    val valuePtr = atPtr + 4
    val prevValue = bbView.getFloat(valuePtr)
    bbView.putFloat(valuePtr, prevValue + value)
  }

  /**
   *  Compares the key contents to the contents stored at pointer externalPtr
   * @param keyContents: the key contents to be compared
   * @param externalPtr: the external pointer where a value is stored that can be compared
   * @return
   */
  def equalsAt(keyContents: Array[Byte], externalPtr: Int): Boolean = {
    equalsAt(keyContents, 0, keyContents.length, externalPtr)
  }

  def equalsAt(
      entry: Array[Byte],
      from: Int,
      len: Int,
      externalPtr: Int): Boolean = {
    var start = externalPtr
    val strLen = bbView.getInt(start)
    if (len != strLen) {
      false
    } else {
      // guards also against integer overflow
      if (start > bytes.length - len - 8) {
        throw new IllegalStateException("Invalid internal buffer contents")
      }
      start += 8 // skip length and value
      var i = 0
      while (i < len && entry(i) == bytes(start)) {
        i += 1
        start += 1
      }
      i == len
    }
  }

  /**
   * Extract the next entry from the hash table
   * @param entry: the entry which containers the next pointer and simultaneously acts as output
   * @param needsKey: whether the key should be extracted
   * @return
   */
  def getEntry(entry: ByteStoreNextEntry, needsKey: Boolean): Boolean = {
    var i = entry.nextPointer
    if (i < ptr) {
      // what is internal pointer for
      entry.currentPointer = entry.nextPointer
      val len = bbView.getInt(i)
      i += Constants.lengthSize
      val value = bbView.getFloat(i)
      i += Constants.valueSize
      entry.value = value

      if (needsKey) {
        val contents = Array.ofDim[Byte](len)
        System.arraycopy(bytes, i, contents, 0, len)
        entry.key = contents
      } else {
        entry.key = null
      }

      i += len
      i = BinaryUtils.align(i, 4)
      entry.nextPointer = i
      true
    } else {
      false
    }
  }
}

/**
 *  An entry from the hash table ByteArrayHashTable indexing ByteArrayStore
 */
class HashEntry {
  var hash: Long = 0L
  var hashTablePtr: Int = 0
  var offsetPayload: Int = 0
  def isFree(): Boolean = {
    offsetPayload == AddEntryStatus.OutOfSpace
  }
}

/**
 * A hash table that stores the offsets for the ByteArrayStore
 * @param size
 */
class OffsetHashTable(size: Int) {
  assert(BinaryUtils.isPowerOf2(size))

  private[OffsetHashTable] val offsets: Array[Int] = Array.ofDim[Int](size)

  def capacity: Int = {
    offsets.length
  }

  /**
   * Returns the next position at which keyContents can be hashed, starting at offset
   * The output is in
   * @param keyContents
   * @param posIndex
   * @param output
   */
  def nextPosition(
      keyContents: Array[Byte],
      posIndex: Int,
      output: HashEntry): Unit = {
    assert(posIndex >= 0)
    val mask = size - 1
    var hash = 0L
    if (posIndex == 0) {
      hash = XXH64.hashUnsafeBytes(
        keyContents,
        Platform.BYTE_ARRAY_OFFSET,
        keyContents.length,
        Constants.hashSeed)

      output.hash = hash
    } else {
      hash = output.hash
    }
    output.hashTablePtr = ((hash + posIndex) & mask).toInt
    output.offsetPayload = getContents(output.hashTablePtr)
  }

  def setContents(hashTablePtr: Int, contents: Int): Unit = {
    assert(contents < Integer.MAX_VALUE - 1)
    offsets(hashTablePtr) = contents + 1
  }

  def getContents(hashTablePtr: Int): Int = {
    offsets(hashTablePtr) - 1
  }
}

case object ByteArrayTopKImpl
    extends AbstractTopKImplFactory[ByteArrayTopKImpl] {

  def rebuildHash(byteStore: ByteArrayStore): OffsetHashTable = {
    val capacity =
      BinaryUtils.adjustToNextPowerOf2(Math.max(4, 2 * byteStore.cnt))
    val newHashes = new OffsetHashTable(capacity)
    val byteStoreEntry = new ByteStoreNextEntry()
    while (byteStore.getEntry(byteStoreEntry, true)) {
      var hashPosition = 0
      val entry = new HashEntry()
      var done = false
      do {
        newHashes.nextPosition(byteStoreEntry.key, hashPosition, entry)
        if (entry.isFree()) {
          newHashes.setContents(
            entry.hashTablePtr,
            byteStoreEntry.currentPointer)
          done = true
        } else {
          hashPosition += 1
        }
      } while (!done && hashPosition < newHashes.capacity)
      assert(done)
    }

    newHashes
  }

  def deserialize(storageFormat: Array[Byte]): ByteArrayTopKImpl = {
    val byteStream = new ByteArrayInputStream(storageFormat)
    val dataStream = new DataInputStream(byteStream)

    val maxSize = dataStream.readInt()
    val ptr = dataStream.readInt()
    val cnt = dataStream.readInt()
    val totalCnt = dataStream.readFloat()
    val numCompactions = dataStream.readFloat()

    val initialSize =
      Math.max(16, BinaryUtils.adjustToNextPowerOf2(ptr))
    val bytes = Array.ofDim[Byte](initialSize)
    dataStream.read(bytes, 0, ptr)
    val byteStore = new ByteArrayStore(initialSize, cnt, bytes, ptr)
    new ByteArrayTopKImpl(maxSize, byteStore, totalCnt, numCompactions)
  }

  def newInstance(maxSize: Int): ByteArrayTopKImpl = {
    new ByteArrayTopKImpl(maxSize)
  }

}

/**
 *   An implementation of top k where the keys are binary arrays
 * @param maxItems: how many items can be stored in the table
 * @param byteStore: a store where keys and values are stored as bytes
 * @param totalCnt: a counter of how many elements have been processed
 * @param numCompactions: how many compactions have happened, those provide an error bound
 */
class ByteArrayTopKImpl(
    maxItems: Int,
    var byteStore: ByteArrayStore,
    var totalCnt: Float,
    var numCompactions: Float)
    extends AbstractTopKImpl[ByteArrayTopKImpl, Array[Byte], Any] {

  assert(maxItems >= 2)
  var hashes: OffsetHashTable = ByteArrayTopKImpl.rebuildHash(byteStore)

  def this(maxItems: Int) {
    this(maxItems, new ByteArrayStore(16), 0.0f, 0.0f)
  }

  def isHashTableHalfFull(): Boolean = {
    val numberOfItems = byteStore.cnt.toLong
    val hashTableCapacity = hashes.capacity.toLong
    2 * numberOfItems > hashTableCapacity
  }

  def growHash(): Unit = {
    val newSize = hashes.capacity << 1
    freeHashes()
    hashes = ByteArrayTopKImpl.rebuildHash(byteStore)
    assert(hashes.capacity >= newSize)
  }

  /**
   * Extract the stored values from the byteStore
   * @return
   */
  def getValuesArray(): Array[Float] = {
    val byteStoreEntry = new ByteStoreNextEntry()
    val cnt = byteStore.cnt
    val values = Array.ofDim[Float](cnt)
    var i = 0
    while (byteStore.getEntry(byteStoreEntry, false)) {
      values(i) = byteStoreEntry.value
      i += 1
    }
    values
  }

  def copyToNewByteStore(
      threshold: Float,
      newByteStore: ByteArrayStore): Unit = {
    val byteStoreEntry = new ByteStoreNextEntry()
    while (byteStore.getEntry(byteStoreEntry, true)) {
      if (byteStoreEntry.value > threshold) {
        val result = newByteStore.addNewEntry(
          byteStoreEntry.key,
          byteStoreEntry.value - threshold)
        assert(result >= 0)
      }
    }
  }

  def freeHashes(): Unit = {
    hashes = null
  }

  def prune(k: Int): Unit = {
    freeHashes() // we don't need them
    val values = getValuesArray()
    java.util.Arrays.sort(values)
    assert(values.length == byteStore.cnt)
    val adjustedK = Math.min(k, values.length / 2)
    assert(adjustedK < values.length)
    val threshold = values(adjustedK)

    val sz = byteStore.bytes.length
    assert(BinaryUtils.isPowerOf2(sz))

    val newByteStore = new ByteArrayStore(sz)
    copyToNewByteStore(threshold, newByteStore)
    numCompactions += threshold
    byteStore = newByteStore

    hashes = ByteArrayTopKImpl.rebuildHash(byteStore)
  }

  def merge(other: ByteArrayTopKImpl): ByteArrayTopKImpl = {
    val byteStoreEntry = new ByteStoreNextEntry()
    val cnt = other.byteStore.cnt
    val keysPtrs = Array.ofDim[Int](cnt)
    val values = Array.ofDim[Float](cnt)
    var i = 0
    while (other.byteStore.getEntry(byteStoreEntry, false)) {
      keysPtrs(i) = byteStoreEntry.currentPointer
      values(i) = byteStoreEntry.value
      i += 1
    }

    val valueSorter = new GenericSorter(
      new DescFloatKeyArrayInterfaceDesc[Int](values, keysPtrs, cnt))
    valueSorter.Sort()

    var j = 0
    while (j < cnt) {
      byteStoreEntry.nextPointer = keysPtrs(j)
      val result = other.byteStore.getEntry(byteStoreEntry, true)
      assert(result)
      insertKeyValue(byteStoreEntry.key, byteStoreEntry.value)
      j += 1
    }
    numCompactions += other.numCompactions
    this
  }

  def insertKeyValue(key: Array[Byte], value: Float): Unit = {
    var status = InsertKeyValueStatus.InProcess
    do {
      status = insertKeyValueHelper(key, value)
      if (status == InsertKeyValueStatus.ByteStoreOutOfSpace) {
        if (byteStore.cnt > 2 * maxItems) {
          prune(maxItems)
        } else {
          if (!byteStore.grow()) {
            if (byteStore.cnt == 0) {
              throw new IllegalStateException(
                "An empty bytestore cannot contain a very large item")
            }
            prune(byteStore.cnt / 2)
          }
        }
      } else if (status == InsertKeyValueStatus.HashTableOutOfSpace) {
        growHash()
      }
    } while (status != InsertKeyValueStatus.Updated)
    totalCnt += value
  }

  def insertKeyValueHelper(key: Array[Byte], value: Float): Int = {
    var posIndex = 0
    val entry = new HashEntry()
    var status = InsertKeyValueStatus.InProcess
    do {
      hashes.nextPosition(key, posIndex, entry)
      if (entry.isFree()) {
        if (isHashTableHalfFull()) {
          status = InsertKeyValueStatus.HashTableOutOfSpace
        } else {
          val byteStoreEntry = byteStore.addNewEntry(key, value)
          if (byteStoreEntry == AddEntryStatus.OutOfSpace) {
            // could not allocate memory for bytes store
            status = InsertKeyValueStatus.ByteStoreOutOfSpace
          } else {
            assert(byteStoreEntry >= 0)
            hashes.setContents(entry.hashTablePtr, byteStoreEntry)
            status = InsertKeyValueStatus.Updated // added new value to hash table and to byte store
          }
        }
      } else {
        val byteStorePtr = entry.offsetPayload // this is a pointer in the bytestore

        if (byteStore.equalsAt(key, byteStorePtr)) {
          // found item, so uptate the value
          byteStore.incrementValueAt(byteStorePtr, value)
          status = InsertKeyValueStatus.Updated // adjusted previous value
        } else {
          posIndex += 1
        }
      }
    } while (status == InsertKeyValueStatus.InProcess && posIndex < hashes.capacity)

    status
  }

  def getSummary(k: Int, bytesToObjectConverter: Array[Byte] => Any)
      : (Array[Any], Array[Float], Float, Float) = {
    val byteStoreEntry = new ByteStoreNextEntry()
    val cnt = byteStore.cnt
    val keysPtrs = Array.ofDim[Int](cnt)
    val values = Array.ofDim[Float](cnt)
    var i = 0
    while (byteStore.getEntry(byteStoreEntry, needsKey = false)) {
      keysPtrs(i) = byteStoreEntry.currentPointer
      values(i) = byteStoreEntry.value
      i += 1
    }

    val valueSorter = new GenericSorter(
      new DescFloatKeyArrayInterfaceDesc[Int](values, keysPtrs, cnt))
    valueSorter.Sort()

    val adjustedK = Math.min(k, cnt)
    val objectKeys = Array.ofDim[Any](adjustedK)

    var j = 0
    while (j < adjustedK) {
      byteStoreEntry.nextPointer = keysPtrs(j)
      val result = byteStore.getEntry(byteStoreEntry, needsKey = true)
      assert(result)
      assert(byteStoreEntry.key != null)
      objectKeys(j) = bytesToObjectConverter(byteStoreEntry.key)
      j += 1
    }
    (objectKeys, values.take(adjustedK), totalCnt, numCompactions)
  }

  def serialize(): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val dataStream = new DataOutputStream(byteStream)
    dataStream.writeInt(maxItems)
    dataStream.writeInt(byteStore.ptr)
    dataStream.writeInt(byteStore.cnt)
    dataStream.writeFloat(totalCnt)
    dataStream.writeFloat(numCompactions)
    dataStream.write(byteStore.bytes, 0, byteStore.ptr)
    byteStream.toByteArray
  }

}
