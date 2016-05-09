package com.wiki.example

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Created by shona on 5/2/16.
  */
object WikiSteam {
  private def processStream(ssc: StreamingContext, server: String, channels: List[String]): Unit = {
    val stream = ssc.receiverStream(new IrcReceiver(server, channels, StorageLevel.MEMORY_ONLY)).cache()

    /** save everything to JDBC */
    stream.foreachRDD { rdd =>
      val data = rdd.collect().map { edit =>
        ("channel" -> edit.channel) ~
          ("comment" -> edit.comment) ~
          ("diff" -> edit.diff) ~
          ("page" -> edit.page) ~
          ("timestamp" -> edit.timestamp.getTime) ~
          ("username" -> edit.username)
      }.toList
      //HttpUtils.logs(compact(render(data)))
      val json = compact(render(data))
      println(data)
    }
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("IRC Wikipedia Page Edit Stream").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sparkContext, Seconds(10))
    ssc.checkpoint("/tmp/spark-checkpoint")

    val channels = if (args.length > 0) sparkContext.textFile(args(0)).collect().toList
    else List("#en.wikisource", "#en.wikibooks", "#en.wikinews", "#en.wikiquote", "#en.wikipedia", "#wikidata.wikipedia")

    processStream(ssc, "irc.wikimedia.org", channels)

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

}
