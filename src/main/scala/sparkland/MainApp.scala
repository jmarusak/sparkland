import org.apache.spark.sql.SparkSession

object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getActiveSession match {
      case Some(session) => session
      case None => SparkSession.builder().master("local[1]").getOrCreate()
    }
    spark.conf.set("spark.sql.shuffle.partitions", 3)

    val df = spark.read.format("json").load("data/flight-data/json/2015-summary.json")
    df.printSchema()
  }
}
