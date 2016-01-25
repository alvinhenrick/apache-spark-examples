package com.tweet.example

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext

/**
 * Created by Alvin on 7/23/15.
 */
object NaiveBayesTest {

  def main(args: Array[String]) {

    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }

    val sc = new SparkContext(master, "NaiveBayesTest", System.getenv("SPARK_HOME"))


    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/shona/IdeaProjects/apache-spark-examples/data/Sentiment Analysis Dataset.csv")

    val labeledVectors = df.select("Sentiment", "SentimentText").map { row =>
      val text = row.getAs[String]("SentimentText")
      val label = row.getAs[String]("Sentiment").toDouble
      LabeledPoint(label, Utils.featurize(text))
    }

    val model = NaiveBayes.train(labeledVectors)
    val test = Utils.featurize("I am very happy today")
    val labelId = model.predict(test)

    println("Label: " + labelId)


    /*
      val data = sc.textFile("/Users/shona/IdeaProjects/apache-spark-examples/data/Sentiment Analysis Dataset.csv")

     val noHeader = data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

     val documents = noHeader.map { line => TextParser.parse(line) }

     val labeledVectors = documents.collect { case Some(doc: Document) =>
       LabeledPoint(doc.label, Utils.featurize(doc.body))
     }

     val model = NaiveBayes.train(labeledVectors)

     val test = Utils.featurize("I am very happy today")

     val labelId = model.predict(test)

     println("Label: " + labelId)*/

  }
}
