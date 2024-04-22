import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types._

case class Transaction(
    timestamp: Timestamp,
    date: Date,
    tid: Integer,
    price: Double,
    sell: Boolean,
    amount: Double
)

val schema = StructType(Array(
  StructField("timestamp", TimestampType, true),
  StructField("date", DateType, true),
  StructField("tid", IntegerType, true),
  StructField("price", DoubleType, false),
  StructField("sell", BooleanType, false),
  StructField("amount", DoubleType, false)))

val dfStream = 
  spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingoffsets", "latest")
    .option("subscribe", "transactions")
    .load()
    .select(
      from_json(col("value").cast("string"), schema)
        .alias("v")).select("v.*").as[Transaction] 
        
val query = 
  dfStream
    .writeStream
    .format("memory")
    .queryName("transactionsStream")
    .outputMode("append")
    .start()

// in separate notebook paragraph
val table = spark.table("transactionsStream").sort($"timestamp")
z.show(table)
