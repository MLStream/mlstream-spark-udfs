package mlstream.udf.topk

import mlstream.udf.topk.impl.{
  AddEntryStatus,
  BinaryUtils,
  ByteArrayStore,
  ByteArrayTopKImpl,
  ByteStoreNextEntry,
  HashEntry,
  InsertKeyValueStatus,
  OffsetHashTable
}
import org.scalatest.FunSpec

import scala.collection.mutable

class GenericTopKImplTest extends FunSpec {
  describe("BinaryUtils") {
    it("isPowerOf2") {
      assert(BinaryUtils.isPowerOf2(2) == true)
      assert(BinaryUtils.isPowerOf2(4) == true)
      assert(BinaryUtils.isPowerOf2(3) == false)
      assert(BinaryUtils.isPowerOf2(17) == false)
    }

    it("adjustToNextPowerOf2") {
      assert(BinaryUtils.adjustToNextPowerOf2(2) == 4)
      assert(BinaryUtils.adjustToNextPowerOf2(4) == 8)
      assert(BinaryUtils.adjustToNextPowerOf2(3) == 4)
      assert(BinaryUtils.adjustToNextPowerOf2(17) == 32)
    }

    it("align") {
      assert(BinaryUtils.align(0, 4) == 0)
      assert(BinaryUtils.align(1, 4) == 4)
      assert(BinaryUtils.align(2, 4) == 4)
      assert(BinaryUtils.align(3, 4) == 4)
      assert(BinaryUtils.align(8, 4) == 8)
      assert(BinaryUtils.align(9, 4) == 12)
    }
  }

  describe("ByteArrayStore") {
    it("whenEmpty") {
      val store = new ByteArrayStore(16)
      assert(store.ptr == 0)
      assert(store.cnt == 0)
    }

    def getEntryAtPtr(
        store: ByteArrayStore,
        ptr: Int): Option[ByteStoreNextEntry] = {
      val out = new ByteStoreNextEntry(ptr)
      if (store.getEntry(out, true)) {
        Some(out)
      } else {
        None
      }
    }

    def extractEntries(store: ByteArrayStore): Seq[(Array[Byte], Float)] = {
      val buffer = new mutable.ArrayBuffer[(Array[Byte], Float)]
      val out = new ByteStoreNextEntry()
      while (store.getEntry(out, true)) {
        buffer += ((out.key, out.value))
      }
      buffer
    }

    it("adding to store when space is available") {
      val items =
        Array(("1", 10.0f), ("23", 11.0f), ("456", 12.0f), ("789", 13.0f))
      val store = new ByteArrayStore(64)
      var nextFreePtr = 0
      for ((key, value) <- items) {
        val keyBytes = key.getBytes
        val ptr = store.addNewEntry(keyBytes, value)
        assert(ptr >= 0)
        assert(nextFreePtr == ptr)

        nextFreePtr += (8 + keyBytes.length)
        nextFreePtr = BinaryUtils.align(nextFreePtr, 4)

        val entry = getEntryAtPtr(store, ptr)
        assert(entry.isDefined)
        assertResult(value) {
          entry.get.value
        }
        assertResult(keyBytes) {
          entry.get.key
        }

        assert(store.equalsAt(keyBytes, ptr))
      }

      val entries = extractEntries(store)
      val strEntries = entries.map { case (k, v) => (new String(k), v) }.toArray
      assertResult(items) { strEntries }
    }

    it("adding to store when space is not available") {
      val store = new ByteArrayStore(16)
      val key = Seq.range(0, 16).map(_.toString).mkString
      val ptr = store.addNewEntry(key.getBytes, 1.0f)
      assert(ptr <= 0)
      assert(ptr == AddEntryStatus.OutOfSpace)
    }

    it("we can grow the store") {
      val store = new ByteArrayStore(64)
      var items =
        Array(("1", 10.0f), ("23", 11.0f), ("456", 12.0f), ("789", 13.0f))
      for ((key, value) <- items) {
        val keyBytes = key.getBytes
        val ptr = store.addNewEntry(keyBytes, value)
        assert(ptr >= 0)
      }
      val bigKey = Seq.range(0, 32).map(_ => "a").mkString
      val bigValue = 64.0f
      items = items ++ Array((bigKey, bigValue))
      val extraPtr0 = store.addNewEntry(bigKey.getBytes, bigValue)
      assert(extraPtr0 < 0)
      store.grow()
      val extraPtr1 = store.addNewEntry(bigKey.getBytes, bigValue)
      assert(extraPtr1 >= 0)

      val entries = extractEntries(store)
      val strEntries = entries.map { case (k, v) => (new String(k), v) }.toArray
      assertResult(items) { strEntries }
    }
  }

  describe("OffsetHashTable") {
    it("empty hash table should report empty for all entries") {
      val ht = new OffsetHashTable(16)
      assert(ht.capacity == 16)
      for (i <- 0 until ht.capacity) {
        assertResult(AddEntryStatus.OutOfSpace)(ht.getContents(i))
      }
    }

    it("hash table should correctly hash") {
      val ht = new OffsetHashTable(16)
      val key = "abc".getBytes
      val out = new HashEntry()
      ht.nextPosition(key, 0, out)
      assert(out.hash != 0)
      assert(out.hashTablePtr > 0)
      assert(out.hashTablePtr < 16)
      assert(out.isFree())

      val contents = 11
      ht.setContents(out.hashTablePtr, contents)
      assert(ht.getContents(out.hashTablePtr) == contents)
    }

    it("hash table should correctly hash on collisions") {
      val sz = 16
      val ht = new OffsetHashTable(sz)
      val key = "abc".getBytes
      val out = new HashEntry()
      ht.nextPosition(key, 0, out)
      assert(out.isFree())
      val ptr = out.hashTablePtr
      for (i <- 1 until sz) {
        ht.nextPosition(key, i, out)
        assertResult((ptr + i) % sz)(out.hashTablePtr)
        assert(out.isFree())
      }
    }

    def addToHash(ht: OffsetHashTable, key: Array[Byte]): Boolean = {
      var done = false
      val out = new HashEntry()
      var i = 0
      while (!done && i < ht.capacity) {
        ht.nextPosition(key, i, out)
        if (out.isFree()) {
          ht.setContents(out.hashTablePtr, 0)
          done = true
        } else {
          i += 1
        }
      }
      done
    }

    it(
      "hash table should have insertion cost less than 4*n (n = table capacity)") {
      def bySize(sz: Int): Unit = {
        val rnd = new util.Random(484)
        val ht = new OffsetHashTable(sz)
        for (i <- 0 until sz / 2) {
          val r = rnd.nextString(32)
          val flag = addToHash(ht, r.getBytes)
          assert(flag)
        }
        var j = 0
        val searchStepsCounts = mutable.HashMap[Int, Int]().withDefault(_ => 0)
        while (j < sz) {
          var t = j
          var foundFreePos = false
          while (!foundFreePos) {
            val contents = ht.getContents(t % sz)
            if (contents < 0) {
              val steps = t - j + 1
              searchStepsCounts(steps) += 1
              foundFreePos = true
            }
            t += 1
          }
          j += 1
        }

        val sortedCounts = searchStepsCounts.toList.sortBy {
          case (streakSize, _) => streakSize
        }
        val expectedInsertCost = sortedCounts.map {
          case (numSteps, cnt) => numSteps * cnt.toDouble
        }.sum
        assert(expectedInsertCost / sz.toDouble < 4.0)
      }

      bySize(8)
      bySize(16)
      bySize(32)
      bySize(256)
      bySize(1024)
      bySize(256 * 1024)
      bySize(1024 * 1024)
    }
  }

  describe("boundary conditions") {
    def aStr(sz: Int): String = {
      (0 to sz).map(_ => "a").mkString
    }

    it("bytestore out of space") {
      val bytesKey = aStr(256).getBytes
      val bytesValue = 1.0f
      val impl = new ByteArrayTopKImpl(16)
      val res = impl.insertKeyValueHelper(bytesKey, bytesValue)
      assert(res == InsertKeyValueStatus.ByteStoreOutOfSpace)
    }

    it("hash table out of space") {
      val bytesKey = aStr(1).getBytes
      val bytesValue = 1.0f
      // 16 items allowed, so the hash table is set to 16
      val impl = new ByteArrayTopKImpl(16)
      // initial capacity is 8
      assert(impl.hashes.capacity == 8)
      // the 5th item make the hash table half-full
      for (i <- 0 until 5) {
        impl.insertKeyValue(i.toString.getBytes, 1.0f)
      }

      // this call will trigger resizing of the hash table
      val res = impl.insertKeyValueHelper(bytesKey, bytesValue)
      assert(res == InsertKeyValueStatus.HashTableOutOfSpace)
    }
  }

}
