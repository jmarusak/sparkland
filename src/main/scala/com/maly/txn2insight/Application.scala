package com.maly.txn2insight

import org.apache.spark.sql.{
  SparkSession
}

object Application extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("Insights")
    .getOrCreate()

  println("+++++ Running... +++++")
  
  //Thread.sleep(100000000)
}
