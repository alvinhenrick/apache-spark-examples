package com.logistic.example

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Alvin on 7/23/15.
 */
object SpamClassification {

  def main(args: Array[String]) {

    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    // Setting up spark context
    val conf = new SparkConf().setMaster(master).setAppName("SpamClassification") // run locally with enough threads
    val sc = new SparkContext(conf)

    val datasetSpam = sc.textFile("/Users/shona/IdeaProjects/apache-spark-examples/data/spam/spam.txt")
    val datasetNorm = sc.textFile("/Users/shona/IdeaProjects/apache-spark-examples/data/spam/normal.txt")

    // Create a HashingTF instance to map email text to vectors of n no. of  features.
    val tf = new HashingTF()
    // Each email is split into words, and each word is mapped to one feature.
    val spam = datasetSpam.map(text => tf.transform(text.split(" ")))
    val normal = datasetNorm.map(text => tf.transform(text.split(" ")))

    // Create LabeledPoint datasets for positive (spam) and negative (normal) examples.
    val positive = spam.map(features => LabeledPoint(1, features))
    val negative = normal.map(features => LabeledPoint(0, features))

    val trainingData = positive.union(negative)

    trainingData.cache()

    val model = new LogisticRegressionWithSGD().run(trainingData)

    // Test on a positive example (spam) and a negative one (normal).
    val posTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))

    val negTest = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))

    println("Prediction for positive test example: " + model.predict(posTest))

    //println("Prediction for message " + model.predict(negTest))
  }
}
