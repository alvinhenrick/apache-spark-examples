package com.wiki.produce.example

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._


/**
  * Created by shona on 5/2/16.
  */
object WikiSteam {

  private def processStream(ssc: StreamingContext, server: String, channels: List[String]): Unit = {
    val stream = ssc.receiverStream(new IrcReceiver(server, channels, StorageLevel.MEMORY_ONLY)).cache()

    /** save everything to Kafka */
    stream.foreachRDD { rdd =>

      rdd.foreachPartition { records =>

        val props = new util.HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          "kafka1.hadoop:9092")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-01")

        records.foreach { record =>
          val producer = new KafkaProducer[String, String](props)
          val message = new ProducerRecord[String, String]("wikitopic", null, record.toJson.compactPrint)
          producer.send(message)
        }

      }
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
