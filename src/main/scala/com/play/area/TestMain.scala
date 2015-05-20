package com.play.area

import com.twitter.example.{Document, TextParser, Tokenizer}

import scala.io.Source

/**
 * Created by Alvin on 4/30/15.
 */
object TestMain {


  def main(args: Array[String]) {

    val reader = Source.fromFile("/Users/shona/IdeaProjects/apache-spark-examples/data/Sentiment Analysis Dataset.csv").getLines()

    val docs = TextParser.parseAll(reader.drop(1))

    val temp = docs collect { case Some(d: Document) => d }

    val termDocs = Tokenizer.tokenizeAll(temp.toIterable)

    termDocs.foreach {
      println
    }

  }
}
