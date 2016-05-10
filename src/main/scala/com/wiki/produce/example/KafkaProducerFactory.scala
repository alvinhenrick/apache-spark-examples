package com.wiki.produce.example

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.log4j.Logger

import scala.collection.mutable


/**
  * Created by shona on 5/10/16.
  */
object KafkaProducerFactory {

  import scala.collection.JavaConversions._

  private val logger = Logger.getLogger(getClass)

  private val producers = mutable.Map[Map[String, String], KafkaProducer[String, String]]()

  def getOrCreateProducer(config: Map[String, String]): KafkaProducer[String, String] = {

    val defaultConfig = Map(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer"
    )

    val finalConfig = defaultConfig ++ config

    producers.getOrElseUpdate(finalConfig, {
      logger.info(s"Create Kafka producer , config: $finalConfig")
      val producer = new KafkaProducer[String, String](finalConfig)

      sys.addShutdownHook {
        logger.info(s"Close Kafka producer, config: $finalConfig")
        producer.close()
      }

      producer
    })
  }
}
