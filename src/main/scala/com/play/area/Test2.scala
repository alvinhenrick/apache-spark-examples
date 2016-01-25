package com.play.area

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by shona on 8/19/15.
 */
object Test2 {

  def main(args: Array[String]) {

    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }

    val sc = new SparkContext(master, "NaiveBayesTest", System.getenv("SPARK_HOME"))

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/shona/IdeaProjects/apache-spark-examples/data/tempdata.csv")

    df.foreach(r => println(r.mkString(",")))


  }

}
