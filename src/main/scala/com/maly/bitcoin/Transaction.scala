package com.maly.bitcoin

import java.sql.{Date, Timestamp}

case class Transaction(
    timestamp: Timestamp,
    date: Date,
    tid: Integer,
    price: Double,
    sell: Boolean,
    amount: Double
)

object Transaction {
  def apply(
      timestamp: Timestamp,
      date: Date,
      tid: Integer,
      price: Double,
      sell: Boolean,
      amount: Double
  ) = new Transaction(
    timestamp = timestamp,
    date = new Date(timestamp.getTime()),
    tid = tid,
    price = price,
    sell = sell,
    amount = amount
  )
}
