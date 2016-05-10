package com.wiki.produce.example

import com.wiki.produce.example.WikiJsonProtocol._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
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
        val props = Map(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "kafka1.hadoop:9092",
          ProducerConfig.CLIENT_ID_CONFIG -> "producer-01")
        val producer = KafkaProducerFactory.getOrCreateProducer(props)
        records.foreach { record =>
          val message = new ProducerRecord[String, String]("wikitopic", record.channel, record.toJson.compactPrint)
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
    ssc.checkpoint("/tmp/wiki/produce/spark-checkpoint")

    val channels = if (args.length > 0) sparkContext.textFile(args(0)).collect().toList
    else List("#en.wikisource", "#en.wikibooks", "#en.wikinews", "#en.wikiquote", "#en.wikipedia", "#wikidata.wikipedia")

    processStream(ssc, "irc.wikimedia.org", channels)

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

}
