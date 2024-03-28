case class Flight(
  DEST_COUNTRY_NAME: String,
  ORIGIN_COUNTRY_NAME: String,
  count: BigInt
)

val df = spark.read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("data/flight-data/csv/2010-summary.csv")

val ds = df.as[Flight]
