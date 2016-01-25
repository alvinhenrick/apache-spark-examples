package com.twitter.example

import jline.console.ConsoleReader
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by Alvin on 5/20/15.
  */
object NaiveBayesClassifier extends App {

  val conf = new SparkConf().setAppName("Tweet Analysis")
    .set("spark.executor.memory", "1g")
    .setMaster("local[4]")
  //.setJars(Seq("target/scala-2.10/dfw-spark-meetup-assembly-0.0.1.jar"))
  val sc = new SparkContext(conf)

  val naiveBayesAndDictionaries = createNaiveBayesModel("/Users/shona/IdeaProjects/apache-spark-examples/data/Sentiment Analysis Dataset.csv")

  console(naiveBayesAndDictionaries)

  /**
    * REPL loop to enter different tweets
    */
  def console(naiveBayesAndDictionaries: NaiveBayesAndDictionaries) = {
    println("Enter 'q' to quit")
    val consoleReader = new ConsoleReader()
    while ( {
      consoleReader.readLine("tweet> ") match {
        case s if s == "q" => false
        case tweet: String =>
          predict(naiveBayesAndDictionaries, tweet)
          true
        case _ => true
      }
    }) {}

    sc.stop()
  }

  /*def main(args: Array[String]) {
    val naiveBayesAndDictionaries = createNaiveBayesModel("/Users/shona/IdeaProjects/apache-spark-examples/data/Sentiment Analysis Dataset.csv")
    predict(naiveBayesAndDictionaries, "I am unhappy with you.")
  }*/

  def predict(naiveBayesAndDictionaries: NaiveBayesAndDictionaries, tweet: String) = {

    // tokenize content and stem it
    val tokens = Tokenizer.tokenize(tweet)
    // compute TFIDF vector
    val tfIdfs = naiveBayesAndDictionaries.termDictionary.tfIdfs(tokens, naiveBayesAndDictionaries.idfs)
    val vector = naiveBayesAndDictionaries.termDictionary.vectorize(tfIdfs)
    val labelId = naiveBayesAndDictionaries.model.predict(vector)

    // convert label from double
    println("Label: " + naiveBayesAndDictionaries.labelDictionary.valueOf(labelId.toInt))
  }

  def createNaiveBayesModel(file: String) = {
    val reader = Source.fromFile(file).getLines()

    val docs = TextParser.parseAll(reader.drop(1))

    val temp = docs collect { case Some(d: Document) => d }

    val termDocs = Tokenizer.tokenizeAll(temp.take(300000).toIterable)

    // put collection in Spark
    val termDocsRdd = sc.parallelize[TermDoc](termDocs.toSeq, 4)

    val numDocs = termDocs.size

    // create dictionary term => id
    // and id => term
    val terms = termDocsRdd.flatMap(_.terms).distinct().collect().sortBy(identity)
    val termDict = new Dictionary(terms)

    val labels = termDocsRdd.flatMap(_.labels).distinct().collect()
    val labelDict = new Dictionary(labels)

    // compute TFIDF and generate vectors
    // for IDF
    val idfs = (termDocsRdd.flatMap(termDoc => termDoc.terms.map((termDoc.doc, _))).distinct().groupBy(_._2) collect {
      case (term, documents) =>
        term -> (numDocs.toDouble / documents.size.toDouble)
    }).collect().toMap

    val tfidfs = termDocsRdd flatMap {
      termDoc => {
        val termPairs = termDict.tfIdfs(termDoc.terms, idfs)
        // we consider here that a document only belongs to the first label
        termDoc.labels.headOption.map {
          label => {
            val labelId = labelDict.indexOf(label).toDouble
            val vector = Vectors.sparse(termDict.count, termPairs)
            LabeledPoint(labelId, vector)
          }
        }
      }
    }

    val model = NaiveBayes.train(tfidfs)
    NaiveBayesAndDictionaries(model, termDict, labelDict, idfs)
  }
}


case class NaiveBayesAndDictionaries(model: NaiveBayesModel,
                                     termDictionary: Dictionary,
                                     labelDictionary: Dictionary,
                                     idfs: Map[String, Double])
