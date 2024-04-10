package com.maly.bitcoin

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.functions.{explode, from_json}
import org.apache.spark.sql.types._

object BatchProducer {
  def jsonToHttpTransaction(
      json: String
  )(implicit spark: SparkSession): Dataset[HttpTransaction] = {
    import spark.implicits._
    val seq = Seq(json)
    val ds: Dataset[String] = seq.toDS()
    val txSchema: StructType = Seq.empty[HttpTransaction].toDS().schema
    val schema = ArrayType(txSchema)
    val arrayColumn = from_json($"value", schema)

    ds.select(explode(arrayColumn).alias("v")).select("v.*").as[HttpTransaction]
  }

  def httpToDomainTransaction(
      ds: Dataset[HttpTransaction]
  )(implicit spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._
    ds.select(
      $"date".cast(LongType).cast(TimestampType).as("timestamp"),
      $"date".cast(LongType).cast(TimestampType).cast(DateType).as("date"),
      $"tid".cast(IntegerType),
      $"price".cast(DoubleType),
      $"type".cast(BooleanType).as("sell"),
      $"amount".cast(DoubleType)
    ).as[Transaction]
  }
}
