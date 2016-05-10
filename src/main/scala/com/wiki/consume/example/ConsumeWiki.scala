package com.wiki.consume.example

import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Created by shona on 5/9/16.
  */
class ConsumeWiki {


  def main(args: Array[String]) {
    /** Spark initialization **/
    val sc = new SparkConf().setAppName("WikiData")
    val ssc = new StreamingContext(sc, Seconds(10)) // this sets the micro batch size

    /** Enable checkpoinging **/
    ssc.checkpoint(".")

    /** Kafka initialisation **/
    val topicsSet = Set("wikitopic")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "kafka1.hadoop:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
  }


}

