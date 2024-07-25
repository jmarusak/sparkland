import org.apache.spark.sql.SparkSession

object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getActiveSession match {
      case Some(session) => session
      case None => SparkSession.builder().master("local[1]").getOrCreate()
    }

    println(spark)
  }
}
