package com.maly.bitcoin

import java.time.Instant
import java.net.{URI, URL}
import scala.concurrent.duration._
import scala.io.Source

import org.apache.spark.sql.{SparkSession, Dataset, SaveMode}
import org.apache.spark.sql.functions.{explode, from_json, lit}
import org.apache.spark.sql.types._

object BatchProducer {

  def processRepeatedly(spark: SparkSession): Unit = {
    val DurationProcessRun: FiniteDuration = 1.minutes
    val DurationBackfill: FiniteDuration = 1.hours 
    
    var timeNow = Instant.now
    val transactions = bitstampTxs("hour")(spark)
    var (startTime, endTime) = processOneBatch(transactions, timeNow.minusSeconds(DurationBackfill.toSeconds), timeNow) 

    val timeProcessEnd = timeNow.plusSeconds(DurationProcessRun.toSeconds) 
    while (endTime.compareTo(timeProcessEnd) < 0) {
      val transactions = bitstampTxs("hour")(spark)
      val (startTimeNext, endTimeNext) = processOneBatch(transactions, startTime, endTime) 
      startTime = startTimeNext
      endTime = endTimeNext
    }
  }

  def bitstampUrl(timeParam: String): URL =
    new URL("https://www.bitstamp.net/api/v2/transactions/btcusd?time=" + timeParam)

  def bitstampTxs(timeParam: String)(spark: SparkSession): Dataset[Transaction] = {
    val url = bitstampUrl(timeParam)
    val json = Source.fromURL(url).mkString
    rawToDomainTransaction(jsonToRawTransaction(json)(spark))(spark)
  }

  def processOneBatch(transactions: Dataset[Transaction], startTime: Instant, endTime: Instant): (Instant, Instant) = {
    val DurationBatchCycle: FiniteDuration = 30.seconds
    val transactionsToSave = filterTxs(transactions, startTime, endTime)
    println(s"Transactions in Dataset[Transaction]: ${transactionsToSave.count}")
    save(transactionsToSave, new URI("./data/bitcoin"))
    Thread.sleep(DurationBatchCycle.toMillis)
    (endTime, Instant.now)
  }

  def filterTxs(
    transactions: Dataset[Transaction],
    startTime: Instant,
    endTime: Instant
  ) = {
    import transactions.sparkSession.implicits._
    transactions.filter(
      ($"timestamp" >= lit(startTime.getEpochSecond).cast(TimestampType)) &&
      ($"timestamp" < lit(endTime.getEpochSecond).cast(TimestampType)))
  }
      
  def jsonToRawTransaction(
      json: String
  )(implicit spark: SparkSession): Dataset[RawTransaction] = {
    import spark.implicits._
    val seq = Seq(json)
    val ds: Dataset[String] = seq.toDS()
    val txSchema: StructType = Seq.empty[RawTransaction].toDS().schema
    val schema = ArrayType(txSchema)
    val arrayColumn = from_json($"value", schema)

    ds.select(explode(arrayColumn).alias("v")).select("v.*").as[RawTransaction]
  }

  def rawToDomainTransaction(
      ds: Dataset[RawTransaction]
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

  def save(transaction: Dataset[Transaction], path: URI): Unit = { 
    transaction.show(1)
    transaction.write
      .mode(SaveMode.Append)
      .partitionBy("date")
      .parquet(path.toString)
  }
}
