import org.apache.spark.sql.functions.{window, col, desc}

spark.conf.set("spark.sql.shuffle.partitions", "5")

val staticDF = spark.read.format("CSV")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("data/retail-data/by-day/2010-12-01.csv")

val schema = staticDF.schema

val streamDF = spark.readStream
  .schema(schema)
  .format("CSV")
  .option("maxFilesPerTrigger", "1")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("data/retail-data/by-day/2010*.csv")

streamDF.isStreaming

val w = streamDF.selectExpr(
  "CustomerId",
  "(UnitPrice * Quantity) as total_cost",
  "InvoiceDate")
  .groupBy(
    col("CustomerId"),
    window(col("InvoiceDate"), "1 day"))
  .sum("total_cost")
  .sort(col("CustomerId"))

w.writeStream
  .format("console")
  .queryName("purchases")
  .outputMode("complete")
  .start()
