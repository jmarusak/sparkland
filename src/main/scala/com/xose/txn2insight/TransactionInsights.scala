package com.xose.txn2insight

import org.apache.spark.sql.{
  SparkSession
}

object TransactionInsights extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("TransactionInsights")
    .getOrCreate()

  println("+++++ Running... +++++")
  
  //Thread.sleep(100000000)
}
