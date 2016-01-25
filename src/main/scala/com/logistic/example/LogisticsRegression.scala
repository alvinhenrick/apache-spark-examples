package com.logistic.example

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by Alvin on 7/23/15.
 */
object LogisticsRegression {


  def main(args: Array[String]) {

    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }

    val sc = new SparkContext(master, "LogisticsRegression", System.getenv("SPARK_HOME"))

    val data = sc.textFile("/Users/shona/IdeaProjects/apache-spark-examples/data/Qualitative_Bankruptcy/Qualitative_Bankruptcy.data.txt")

    println(data.count())

    val parsedData = data.map { line =>
      val parts = line.split(",")
      LabeledPoint(getDoubleValue(parts(6)), Vectors.dense(parts.slice(0, 6).map(x => getDoubleValue(x))))
    }
    parsedData.take(10)

    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)

    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainingData)


    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count

    val met = new BinaryClassificationMetrics(labelAndPreds)

    //println("*******precisionByThreshold**********")

    met.precisionByThreshold().collect().foreach(print(_))

    //println("*******recallByThreshold**********")

    //met.recallByThreshold().collect().foreach(print(_))

    //println("********PR*********")

    //met.pr().collect().foreach(print(_))

    //println(trainErr)
  }

  def getDoubleValue(input: String): Double = input match {
    case "P" => 3.0
    case "A" => 2.0
    case "N" | "NB" => 1.0
    case "B" => 0.0
  }
}
