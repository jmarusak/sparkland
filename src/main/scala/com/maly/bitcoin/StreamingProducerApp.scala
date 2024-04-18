package com.maly.bitcoin

import StreamingProducer._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.JavaConversions._

object StreamingProducerApp extends App {
  val topic = "transactions"
  
  val properties = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")
 
  val kafkaProducer = new KafkaProducer[String, String](properties)

  val jsonRaw =
    """{"amount":"0.00172969","date":"1712675895","price":"68951","tid":"332782270","type":"0"}]"""
  val txn: Transaction = rawToDomainTransaction(jsonToRawTransaction(jsonRaw))
  val jsonMessage: String = serializeTransaction(txn)

  println(jsonMessage)
  kafkaProducer.send(new ProducerRecord(topic, jsonMessage))
  kafkaProducer.close()
}

