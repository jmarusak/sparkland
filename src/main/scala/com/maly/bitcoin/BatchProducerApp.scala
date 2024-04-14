package com.maly.bitcoin

import org.apache.spark.sql.SparkSession

object BatchProducerApp extends App {
  val spark: SparkSession = SparkSession.builder.master("local[1]").appName("BatchProducer").getOrCreate()

  BatchProducer.processRepeatedly(spark)
}
