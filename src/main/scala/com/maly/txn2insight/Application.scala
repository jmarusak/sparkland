package com.maly.txn2insight

import org.apache.spark.sql.{
  SparkSession
}

object Application extends App {
  
  val source: String = "./data/source/"
  val target: String = "./data/delta/"

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("Insights")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config( "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()

  val data = List(("adam", 1), ("peter", 2))
  val df = spark.createDataFrame(data)
  df.show(2)


  println("+++++ Running... +++++")
  
  //Thread.sleep(100000000)
}
