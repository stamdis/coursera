package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 3 //45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  val correctData = new {
    val testDataFilename = "src/main/resources/stackoverflow/StackOverflowTestData.csv"
    val groupedResults = Set(
      (0,Set(
        Posting(2,1,None,Some(0),1,Some("C#")),
        Posting(2,2,None,Some(0),2,Some("C#")),
        Posting(2,3,None,Some(0),3,Some("C#")),
        Posting(2,4,None,Some(0),5,None))
        .map((Posting(1,0,Some(3),None,1,Some("C#")), _))),
      (5,Set(
        Posting(2,6,None,Some(5),1,Some("Java")),
        Posting(2,7,None,Some(5),2,Some("Java")),
        Posting(2,8,None,Some(5),3,Some("Java")),
        Posting(2,9,None,Some(5),4,None))
        .map((Posting(1,5,Some(8),None,1,Some("Java")), _))),
      (10,Set(
        Posting(2,11,None,Some(10),1,Some("Scala")),
        Posting(2,12,None,Some(10),2,Some("Scala")),
        Posting(2,13,None,Some(10),3,Some("Scala")),
        Posting(2,14,None,Some(10),6,None))
        .map((Posting(1,10,Some(13),None,1,Some("Scala")), _))))
    val scoredResults = Set(
      (Posting(1,0,Some(3),None,1,Some("C#")),5),
      (Posting(1,5,Some(8),None,1,Some("Java")),4),
      (Posting(1,10,Some(13),None,1,Some("Scala")),6))
    val vectorResults = Set((4,5), (1,4), (10,6)).map(x => (x._1 * StackOverflow.langSpread, x._2))
    val kMeansResults = Set((4,5), (1,4), (10,6)).map(x => (x._1 * StackOverflow.langSpread, x._2))
    val results = Array(("Java", 100.0, 1, 4), ("C#", 100.0, 1, 5), ("Scala", 100.0, 1, 6))
  }

  val grouped = {
    val lines = StackOverflow.sc.textFile(correctData.testDataFilename)
    val raw = testObject.rawPostings(lines)
    testObject.groupedPostings(raw)
  }
  val scored  = testObject.scoredPostings(grouped)
  val vectors = testObject.vectorPostings(scored)
  val means   = testObject.kmeans(vectors.collect(), vectors, debug = true)
  val results = testObject.clusterResults(means, vectors)

  override def afterAll(): Unit = {
    StackOverflow.sc.stop()
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("'groupedPostings' should return correct results") {
    val results = grouped.collect().map(x => (x._1, x._2.toSet)).toSet
    assertResult(correctData.groupedResults)(results)
  }

  test("'scoredPostings' should return correct results") {
    val results = scored.collect().toSet
    assertResult(correctData.scoredResults)(results)
  }

  test("'vectorPostings' should return correct results") {
    val results = vectors.collect().toSet
    assertResult(correctData.vectorResults)(results)
  }

  test("'kmeans' should return correct results") {
    val results = means.toSet
    assertResult(correctData.kMeansResults)(results)
  }

  test("'clusterResults' should return correct results") {
    val results = this.results
    assertResult(correctData.results)(results)
  }
}
