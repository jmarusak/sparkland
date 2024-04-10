package com.maly.sparkland

import org.apache.spark.sql.SparkSession

object BasicApp extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("BasicApp")
    .getOrCreate()

  val data = Seq(("adam", 1), ("peter", 2))
  val df = spark.createDataFrame(data)
  df.show()

  // Thread.sleep(100000000)
}
