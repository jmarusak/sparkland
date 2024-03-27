case class Flight(
  DEST_COUNTRY_NAME: String,
  ORIGIN_COUNTRY_NAME: String,
  count: BigInt
)

val df = spark.read.parquet("data/flight-data/parquet/2010-summary.parquet")
val ds = df.as[Flight]
