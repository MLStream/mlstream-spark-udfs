package mlstream.utils.genericsort

/* The following source code was derived from Golang and carries Golang's LICENSE.
Source code origin: https://golang.org/src/sort/sort.go
Golang's LICENSE from https://golang.org/LICENSE

Copyright (c) 2009 The Go Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
 * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
  Why not java sorts? We want a sort which does not box/unbox input all the time
 */

trait Interface {
  def Len(): Int

  // Less reports whether the element with
  // index i should sort before the element with index j.
  def Less(i: Int, j: Int): Boolean

  // Swap swaps the elements with indexes i and j.
  def Swap(i: Int, j: Int): Unit
}

class OutputTuple(var fst: Int, var snd: Int)

final class GenericSorter[Iface <: Interface](data: Iface) {
  def insertionSort(a: Int, b: Int): Unit = {
    var i = a + 1
    while (i < b) {
      var j = i
      while (j > a && data.Less(j, j - 1)) {
        data.Swap(j, j - 1)
        j -= 1
      }
      i += 1
    }
  }

  // medianOfThree moves the median of the three values data[m0], data[m1], data[m2] into data[m1].
  def medianOfThree(m1: Int, m0: Int, m2: Int): Unit = {
    // sort 3 elements
    if (data.Less(m1, m0)) {
      data.Swap(m1, m0)
    }
    // data[m0] <= data[m1]
    if (data.Less(m2, m1)) {
      data.Swap(m2, m1)
      // data[m0] <= data[m2] && data[m1] < data[m2]
      if (data.Less(m1, m0)) {
        data.Swap(m1, m0)
      }
    }
    // now data[m0] <= data[m1] <= data[m2]
  }

  def doPivot(lo: Int, hi: Int, out: OutputTuple): Unit = {
    // Written like this to avoid integer overflow.
    val m = (hi - lo) / 2 + lo
    if (hi - lo > 40) {
      // Tukey's ``Ninther,'' median of three medians of three.
      val s = (hi - lo) / 8
      medianOfThree(lo, lo + s, lo + 2 * s)
      medianOfThree(m, m - s, m + s)
      medianOfThree(hi - 1, hi - 1 - s, hi - 1 - 2 * s)
    }
    medianOfThree(lo, m, hi - 1)

    // Invariants are:
    //	data[lo] = pivot (set up by ChoosePivot)
    //	data[lo < i < a] < pivot
    //	data[a <= i < b] <= pivot
    //	data[b <= i < c] unexamined
    //	data[c <= i < hi-1] > pivot
    //	data[hi-1] >= pivot
    val pivot = lo
    var a = lo + 1
    var c = hi - 1

    while (a < c && data.Less(a, pivot)) {
      a += 1
    }
    var b = a
    var cont1 = true
    while (cont1) {
      while (b < c && !data.Less(pivot, b)) { // data[b] <= pivot
        b += 1
      }
      while (b < c && data.Less(pivot, c - 1)) { // data[c-1] > pivot
        c -= 1
      }
      if (b >= c) {
        cont1 = false
      } else {
        // data[b] > pivot; data[c-1] <= pivot
        data.Swap(b, c - 1)
        b += 1
        c -= 1
      }
    }
    var protect = hi - c < 5
    if (!protect && hi - c < (hi - lo) / 4) {
      // Lets test some points for equality to pivot
      var dups = 0
      if (!data.Less(pivot, hi - 1)) { // data[hi-1] = pivot
        data.Swap(c, hi - 1)
        c += 1
        dups += 1
      }

      if (!data.Less(b - 1, pivot)) { // data[b-1] = pivot
        b -= 1
        dups += 1
      }

      // m-lo = (hi-lo)/2 > 6
      // b-lo > (hi-lo)*3/4-1 > 8
      // ==> m < b ==> data[m] <= pivot
      if (!data.Less(m, pivot)) { // data[m] = pivot
        data.Swap(m, b - 1)
        b -= 1
        dups += 1
      }
      // if at least 2 points are equal to pivot, assume skewed distribution
      protect = dups > 1
    }

    if (protect) {
      // Protect against a lot of duplicates
      // Add invariant:
      //	data[a <= i < b] unexamined
      //	data[b <= i < c] = pivot
      var cont2 = true
      while (cont2) {
        while (a < b && !data.Less(b - 1, pivot)) { // data[b] == pivot
          b -= 1
        }
        while (a < b && data.Less(a, pivot)) { // data[a] < pivot
          a += 1
        }
        if (a >= b) {
          cont2 = false
        } else {
          // data[a] == pivot; data[b-1] < pivot
          data.Swap(a, b - 1)
          a += 1
          b -= 1
        }
      }
    }
    // Swap pivot into middle
    data.Swap(pivot, b - 1)
    //val tuple = ((b - 1).toLong << 32) | c
    (b - 1, c)
    out.fst = b - 1
    out.snd = c
  }

  // siftDown implements the heap property on data[lo, hi).
  // first is an offset into the array where the root of the heap lies.
  def siftDown(lo: Int, hi: Int, first: Int): Unit = {
    var root = lo
    var cont = true
    while (cont) {
      var child = 2 * root + 1
      if (child >= hi) {
        cont = false
      } else {
        if (child + 1 < hi && data.Less(first + child, first + child + 1)) {
          child += 1
        }
        if (!data.Less(first + root, first + child)) {
          cont = false
        } else {
          data.Swap(first + root, first + child)
          root = child
        }
      }
    }
  }

  def heapSort(a: Int, b: Int) {
    val first = a
    val lo = 0
    val hi = b - a

    // Build heap with greatest element at top.
    var i = (hi - 1) / 2
    while (i >= 0) {
      siftDown(i, hi, first)
      i -= 1
    }
    var j = hi - 1
    // Pop elements, largest first, into end of data.
    while (j >= 0) {
      data.Swap(first, first + j)
      siftDown(lo, j, first)
      j -= 1
    }
  }

  def quickSort(aInp: Int, bInp: Int, maxDepthInp: Int): Unit = {
    var a = aInp
    var b = bInp
    var maxDepth = maxDepthInp
    val tpl = new OutputTuple(0, 0)
    while (b - a > 12) { // Use ShellSort for slices <= 12 elements
      if (maxDepth == 0) {
        heapSort(a, b)
        return
      }
      maxDepth -= 1
      doPivot(a, b, tpl)
      val mlo = tpl.fst
      val mhi = tpl.snd
      // Avoiding recursion on the larger subproblem guarantees
      // a stack depth of at most lg(b-a).
      if (mlo - a < b - mhi) {
        quickSort(a, mlo, maxDepth)
        a = mhi // i.e., quickSort(data, mhi, b)
      } else {
        quickSort(mhi, b, maxDepth)
        b = mlo // i.e., quickSort(data, a, mlo)
      }
    }
    if (b - a > 1) {
      // Do ShellSort pass with gap 6
      // It could be written in this simplified form cause b-a <= 12
      var i = a + 6
      while (i < b) {
        if (data.Less(i, i - 6)) {
          data.Swap(i, i - 6)
        }
        i += 1
      }
      insertionSort(a, b)
    }
  }

  // maxDepth returns a threshold at which quicksort should switch
  // to heapsort. It returns 2*ceil(lg(n+1)).
  def maxDepth(n: Int): Int = {
    var depth: Int = 0
    var i = n
    while (i > 0) {
      depth += 1
      i >>= 1
    }
    depth * 2
  }

  def Sort(): Unit = {
    val n = data.Len()
    quickSort(0, n, maxDepth(n))
  }

  //For median partitioner
  def quickSortForMedianPartitioner(
      aInp: Int,
      bInp: Int,
      maxDepthInp: Int,
      partIndex: Int): Unit = {
    if (!(partIndex >= aInp && partIndex < bInp)) {
      return
    }
    var a = aInp
    var b = bInp
    var maxDepth = maxDepthInp
    val tpl = new OutputTuple(0, 0)
    while (b - a > 12) { // Use ShellSort for slices <= 12 elements
      if (maxDepth == 0) {
        heapSort(a, b)
        return
      }
      maxDepth -= 1
      doPivot(a, b, tpl)
      val mlo = tpl.fst
      val mhi = tpl.snd
      // Avoiding recursion on the larger subproblem guarantees
      // a stack depth of at most lg(b-a).
      if (mlo - a < b - mhi) {
        quickSortForMedianPartitioner(a, mlo, maxDepth, partIndex)
        a = mhi // i.e., quickSort(data, mhi, b)
      } else {
        quickSortForMedianPartitioner(mhi, b, maxDepth, partIndex)
        b = mlo // i.e., quickSort(data, a, mlo)
      }
    }
    if (b - a > 1) {
      // Do ShellSort pass with gap 6
      // It could be written in this simplified form cause b-a <= 12
      var i = a + 6
      while (i < b) {
        if (data.Less(i, i - 6)) {
          data.Swap(i, i - 6)
        }
        i += 1
      }
      insertionSort(a, b)
    }
  }

  def Partition(k: Int): Unit = {
    val n = data.Len()
    assert(k >= 0 && k < n)
    quickSortForMedianPartitioner(0, n, maxDepth(n), k)
  }

}
