package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.junit._
import org.junit.Assert.assertEquals
import java.io.File

import stackoverflow.StackOverflow.{groupedPostings, rawPostings, sc, scoredPostings, vectorPostings}

object StackOverflowSuite {
  lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  lazy val sc: SparkContext = new SparkContext(conf)

  val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
  val raw     = rawPostings(lines)
  val grouped = groupedPostings(raw)
  val scored  = scoredPostings(grouped)
  val vectors = vectorPostings(scored)
}

class StackOverflowSuite {
  import StackOverflowSuite._


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
//    override def kmeansKernels = 45
    override def kmeansKernels = 2
    override def kmeansEta: Double = 20.0D
//    override def kmeansMaxIterations = 120
    override def kmeansMaxIterations = 1

  }

  @Test def `testObject can be instantiated`: Unit = {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }
//  import StackOverflow._

  @Test def `test clusterResults`: Unit = {
    var testCenters: Array[(Int, Int)] =
      Array(
        (0, 0),
        (50000, 50000),
        (100000, 100000),
        (150000, 150000),
        (200000, 200000),
        (250000, 250000),
        (300000, 300000)
      )
    var testPoints: RDD[(LangIndex, HighScore)] =
      StackOverflow.sc.parallelize(Array(
        (0, 0),
        (0, 0),
        (0, 0),
        (50000, 50000),
        (50000, 50000),
        (50000, 50000),
        (100000, 100000),
        (100000, 100000),
        (100000, 100000),
        (200000, 200000),
        (200000, 200000),
        (200000, 200000),
        (300000, 300000),
        (300000, 300000),
        (300000, 300000),
        (250000, 250000),
        (250000, 250000),
        (250000, 250000),
        (150000, 150000),
        (150000, 150000),
        (150000, 150000),
      ))
    val res = testObject.clusterResults(testCenters, testPoints)
    val resString: String = res.map(elem => elem.toString()).mkString("(", ", ", ")")
    val expectedVal = List(
      "(",
      "(JavaScript,100.0,3,0)",
      ", (Java,100.0,3,50000)",
      ", (PHP,100.0,3,100000)",
      ", (Python,100.0,3,150000)",
      ", (C#,100.0,3,200000)",
      ", (C++,100.0,3,250000)",
      ", (Ruby,100.0,3,300000)",
      ")"
    )
    val expectedString: String = expectedVal.foldLeft("")(_ + _)
    println(expectedString)
    println(resString)
    Assert.assertEquals(expectedString, resString)
  }

  @Test def `test clusterResults simple`: Unit = {
    var testCenters: Array[(Int, Int)] =
      Array(
        (0, 0),
        (50000, 50000),
        (100000, 100000),
        (150000, 150000),
        (200000, 200000),
        (250000, 250000),
        (300000, 300000)
      )
    var testPoints: RDD[(LangIndex, HighScore)] =
      StackOverflow.sc.parallelize(Array(
        (0, 0),
        (0, 0),
        (0, 0),
        (50000, 50000),
        (50000, 50000),
        (50000, 50000),
        (100000, 100000),
        (100000, 100000),
        (100000, 100000),
        (200000, 200000),
        (200000, 200000),
        (200000, 200000),
        (300000, 300000),
        (300000, 300000),
        (300000, 300000),
        (250000, 250000),
        (250000, 250000),
        (250000, 250000),
        (150000, 150000),
        (150000, 150000),
        (150000, 150000),
      ))
    val res = testObject.clusterResults(testCenters, testPoints)
    val resString: String = res.map(elem => elem.toString()).mkString("(", ", ", ")")
    val expectedVal = List(
      "(",
      "(JavaScript,100.0,3,0)",
      ", (Java,100.0,3,50000)",
      ", (PHP,100.0,3,100000)",
      ", (Python,100.0,3,150000)",
      ", (C#,100.0,3,200000)",
      ", (C++,100.0,3,250000)",
      ", (Ruby,100.0,3,300000)",
      ")"
    )
    val expectedString: String = expectedVal.foldLeft("")(_ + _)
    println("expected" + expectedString)
    println("actual" + resString)
    Assert.assertEquals(expectedString, resString)
  }

  def nestedArrayToString(nestedArray: Array[(Int, Int)]): String = {
    nestedArray.map(el => el.toString()).mkString("(", ", ", ")")
  }

  @Test def `kmean simple test`: Unit = {
    val mean = Array((100,100))
    val points = StackOverflow
      .sc
      .parallelize(Array((200, 200), (400, 400), (600, 600)))
    val kmeansResults = testObject.kmeans(mean, points, 2)
    val actualString =
      kmeansResults.map(elem => elem.toString()).mkString("(", ", ", ")")

    val expected = "((400,400))"
    Assert.assertEquals(expected, actualString)
  }

  @Test def `kmean 2 Center test`: Unit = {
    val mean = Array((100,100), (500, 500))
    val points = StackOverflow
      .sc
      .parallelize(Array((200, 200), (400, 400), (800, 800)))
    val kmeansResults = testObject.kmeans(mean, points, 1, debug=true)
    val actualString =
      kmeansResults.map(elem => elem.toString()).mkString("(", ", ", ")")

//    val inter1: RDD[(Int, Iterable[(Int, Int)])] =
//      points.map(v => (testObject.findClosest(v, mean), v)).groupByKey.sortByKey(ascending = true).persist()
//
//
//    println(inter1.collect.map(elem => elem.toString()).mkString("(", ", ", ")"))
//
//    val inter2 = inter1.sortByKey(ascending = true).map(x => x._2)
//    println(inter2.collect.map(el => el.toString()).mkString("(", ", ", ")"))
//
//    val inter3 = inter2.map(kv => testObject.averageVectors(kv))
//    println(inter3.collect.map(el => el.toString()).mkString("(", ", ", ")"))
//
//    val allInter = points.map(v => (testObject.findClosest(v, mean), v)).groupByKey.sortByKey(ascending = true)
//      .map(kv => testObject.averageVectors(kv._2)).collect
//    println(allInter.map(el => el.toString()).mkString("(", ", ", ")"))

    val expected = "((200,200), (600,600))"
    Assert.assertEquals(expected, actualString)
  }

  @Test def `test newmean calc`: Unit = {
    val mean = Array((100,100), (500, 500))
    val points = StackOverflow
      .sc
      .parallelize(Array((200, 200), (400, 400), (800, 800)))

    val newMeans = testObject.calculateNewMeans(mean, points)
    Assert.assertEquals(nestedArrayToString(newMeans), "((200,200), (600,600))")
  }

  @Test def `test newMean will return same size`: Unit = {
    val means = Array((100, 100), (500, 500), (1000, 1000))
    val points = StackOverflow
      .sc
      .parallelize((Array((200, 200), (300, 300), (400, 400))))
    val newMeans = testObject.calculateNewMeans(means, points)
    Assert.assertEquals(means.length, newMeans.length)
  }

  @Test def `test newmean calc returns same length`: Unit = {
    val mean = Array((100, 100), (500, 500), (1000, 1000))
  }

  @Rule def individualTestTimeout = new org.junit.rules.Timeout(100 * 1000)
}
