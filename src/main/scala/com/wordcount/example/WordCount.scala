/**
 * Illustrates flatMap + countByValue for wordcount.
 */
package com.wordcount.example

import org.apache.spark._

/**
 * Created by Alvin on 5/20/15.
 */

object WordCount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    // Create a Scala Spark Context.
    val sc = new SparkContext("local[1]", "wordCount")
    // Load our input data.
    val input = sc.textFile(inputFile)
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)
  }
}
