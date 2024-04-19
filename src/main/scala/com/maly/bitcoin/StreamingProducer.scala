package com.maly.bitcoin

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone
import java.net.URL

import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.io.Source
import scala.collection.JavaConverters._

object StreamingProducer {
  val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
 
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    m.setDateFormat(sdf)
  }

  def bitstampUrl(timeParam: String): URL =
    new URL("https://www.bitstamp.net/api/v2/transactions/btcusd?time=" + timeParam)

  def bitstampTxs(timeParam: String): List[String] = {
    val url = bitstampUrl(timeParam)
    val json = Source.fromURL(url).mkString
    jsonStringArrayToListOfStrings(json) 
  }

  def jsonStringArrayToListOfStrings(json: String): List[String] = {
    val jsonNode: JsonNode = mapper.readTree(json)
    jsonNode.elements().asScala.map(_.toString).toList.sorted
  }

  def jsonToRawTransaction(s: String): RawTransaction = {
    mapper.readValue(s, classOf[RawTransaction])
  }

  def rawToDomainTransaction(txn: RawTransaction): Transaction = {
    Transaction(
      timestamp = new Timestamp(txn.date.toLong * 1000),
      tid = txn.tid.toInt,
      price = txn.price.toDouble,
      sell = if (txn.`type` == "1") true else false,
      amount = txn.amount.toDouble)
  }

  def serializeTransaction(txn: Transaction): String = {
    mapper.writeValueAsString(txn)
  }
}
