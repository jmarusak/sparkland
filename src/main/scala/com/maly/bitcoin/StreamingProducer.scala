package com.maly.bitcoin

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object StreamingProducer {
  val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
 
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    m.setDateFormat(sdf)
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
