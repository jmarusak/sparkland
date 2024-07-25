package bitcoin

import StreamingProducer._

import scala.concurrent.duration._
import scala.collection.JavaConversions._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object StreamingProducerApp extends App {
  val DurationPushSleep: FiniteDuration = 10.seconds

  val topic = "transactions"
  
  val props = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")
 
  val kafkaProducer = new KafkaProducer[String, String](props)

  val jsonRawList: List[String] = bitstampTxs("Minute")
  for (jsonRaw <- jsonRawList) yield {
    val txn: Transaction = rawToDomainTransaction(jsonToRawTransaction(jsonRaw))
    val message: String = serializeTransaction(txn)
  
    println(message)
    kafkaProducer.send(new ProducerRecord(topic, message))
    Thread.sleep(DurationPushSleep.toMillis)
  }
  kafkaProducer.close()
}

