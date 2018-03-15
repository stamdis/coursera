package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  lazy val genHeap: Gen[H] = oneOf(
    const(empty),
    for {
      a <- arbitrary[Int]
      m <- oneOf(const(empty), genHeap)
    } yield insert(a, m)
  )
  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("obj1") = forAll { (a1: Int, a2: Int) =>
    val h =  insert(a2, insert(a1, empty))
    findMin(h) == a1.min(a2)
  }

  property("obj2") = forAll { (a: Int) =>
    val h =  deleteMin(insert(a, empty))
    isEmpty(h)
  }

  def getSortedSeq(heap: H): Seq[A] = {
    if (heap == empty)
      Seq()
    else
      findMin(heap) +: getSortedSeq(deleteMin(heap))
  }

  property("obj3") = forAll { (h: H) =>
    val sortedSeq = getSortedSeq(h)
    sortedSeq == sortedSeq.sorted
  }

  val genNonEmptyHeap = genHeap.filter(!isEmpty(_))
  property("obj4") = forAll(genNonEmptyHeap,genNonEmptyHeap) { (h1: H, h2: H) =>
    val min = findMin(meld(h1, h2))
    min == findMin(h1) || min == findMin(h2)
  }

  property("obj5") = forAll { (set: Seq[Int]) =>
    val h = set.foldLeft(empty)((h, a) => insert(a, h))
    getSortedSeq(h) == set.sorted
  }
}
