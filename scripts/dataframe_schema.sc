import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

val schema = StructType(Array(
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", IntegerType, true)
))

val df = spark.read.format("json").schema(schema).load("data/flight-data/json/2015-summary.json")

val schema2 = StructType(Array(
  StructField("name", StringType, true),
  StructField("age", IntegerType, true)
))

val data = Seq(
  Row("peter", 10),
  Row("john", 23)
)

val myRdd = spark.sparkContext.parallelize(data)
val df2 = spark.createDataFrame(myRdd, schema2)

