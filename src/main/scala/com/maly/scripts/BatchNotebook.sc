val transactions = spark.read.parquet("./data/bitcoin")
transactions.show(1)

val group = transactions.groupBy(window($"timestamp", "1 day"))
val aggregate = group.agg(
  count("tid").as("count"),
  avg("price").as("price"))

val dailyAggregate = aggregate.select("window.start", "count", "price").sort("start")
dailyAggregate.show()
