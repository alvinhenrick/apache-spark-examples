package com.play.area

import org.apache.spark.SparkContext

/**
 * Created by Alvin on 4/30/15.
 */
object TestMain {


  def main(args: Array[String]) {

    /*val reader = Source.fromFile("/Users/shona/IdeaProjects/apache-spark-examples/data/Sentiment Analysis Dataset.csv").getLines()

    val docs = TextParser.parseAll(reader.drop(1))

    val temp = docs collect { case Some(d: Document) => d }

    val termDocs = Tokenizer.tokenizeAll(temp.toIterable)

    termDocs.foreach {
      println
    }*/

    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "MBA", System.getenv("SPARK_HOME"))

    val abandoned = sc.textFile("/Users/shona/IdeaProjects/apache-spark-examples/data/products", 1)

    val data = abandoned.map { line =>
      val items = line.split(",")
      (items(0), items(1))
    }

    val test = data.groupByKey().mapValues(f => f.seq.toList)


    val pageview = sc.textFile("/Users/shona/IdeaProjects/apache-spark-examples/data/pageview", 1)

    val pagedata = pageview.map { line =>
      val items = line.split(",")
      (items(0), items(1), items(2))
    }

    val test2 = pagedata.groupBy(_._1).mapValues { x => x.toSeq.sortBy(_._3) }

    test2.foreach {
      println
    }

  }


}
