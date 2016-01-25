package com.mba.example

import org.apache.spark.SparkContext

/**
 * Created by Alvin on 7/22/15.
 */
object MBA {


  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "MBA", System.getenv("SPARK_HOME"))

    val transactions = sc.textFile("/Users/shona/IdeaProjects/apache-spark-examples/data/transactions", 1)

    transactions.saveAsTextFile("/Users/shona/IdeaProjects/apache-spark-examples/data/rules/output/1")

    val patterns = transactions.flatMap { line =>
      val items = line.split(",").sorted.toList
      processTransaction(items).map(x => (x, 1))
    }

    patterns.saveAsTextFile("/Users/shona/IdeaProjects/apache-spark-examples/data/rules/output/2")

    val combined = patterns.reduceByKey { case (x, y) => x + y }

    combined.saveAsTextFile("/Users/shona/IdeaProjects/apache-spark-examples/data/rules/output/3")

    val subpatterns = combined.flatMap { case (list, freq) =>
      list match {
        case sub if sub.size == 1 => List((list, (null, freq)))
        case sub => {
          val temp = sub.zipWithIndex.map {
            case (e, i) => (list.filter(_ != e), (list, freq))
          }
          temp.::((list, (null, freq)))
        }
      }
    }

    subpatterns.saveAsTextFile("/Users/shona/IdeaProjects/apache-spark-examples/data/rules/output/4")

    val rules = subpatterns.groupByKey();

    rules.saveAsTextFile("/Users/shona/IdeaProjects/apache-spark-examples/data/rules/output/5")

    val assocRules = rules.flatMap { case (fromList, y) =>
      val toList = y.filter(x => x._1 != null)
      val fromCount = y.filter(x => x._1 == null)

      toList.map { case (ele, freq) =>
        val confidence: Double = freq.asInstanceOf[Double] / fromCount.head._2.asInstanceOf[Double]
        (fromList, ele.filterNot(x => fromList.contains(x)), confidence)
      }
    }

    assocRules.saveAsTextFile("/Users/shona/IdeaProjects/apache-spark-examples/data/rules/output/6")
  }

  def processTransaction(xs: List[String]): Seq[List[String]] = {
    1 to xs.length flatMap (x => xs.combinations(x))
  }

  /*
  import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

  object Sort {

    case class Record(name:String, day: String, kind: String, city: String, prize:Int)

    // Define your data

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
        .setAppName("Test")
        .setMaster("local")
        .set("spark.executor.memory", "2g")
      val sc = new SparkContext(conf)
      val rs = sc.parallelize(recs)

      // Generate pair RDD neccesary to call groupByKey and group it
      val key: RDD[((String, String, String), Iterable[Record])] = rs.keyBy(r => (r.day, r.city, r.kind)).groupByKey

      // Once grouped you need to sort values of each Key
      val values: RDD[((String, String, String), List[Record])] = key.mapValues(iter => iter.toList.sortBy(_.prize))

      // Print result
      values.collect.foreach(println)
    }
}
   */

}




