import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types._

case class HttpTransaction(
    amount: String,
    date: String,
    price: String,
    tid: String,
    `type`: String
)

val json =
  """[{"amount":"0.00172969","date":"1712675895","price":"68951","tid":"332782270","type":"0"},
   |{"amount":"0.00707741","date":"1712675892","price":"68948","tid":"332782264","type":"0"}]""".stripMargin

// jsonToHttpTransaction method
import spark.implicits._
val seq = Seq(json)
val ds: Dataset[String] = seq.toDS()
val txSchema: StructType = Seq.empty[HttpTransaction].toDS().schema
val schema = ArrayType(txSchema)
val arrayColumn = from_json($"value", schema)

val ds2 =
  ds.select(explode(arrayColumn).alias("v")).select("v.*").as[HttpTransaction]
