import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, expr}

object MainApp {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.getActiveSession match {
      case Some(session) => session
      case None => SparkSession.builder().master("local[1]").getOrCreate()
    }
    session.conf.set("spark.sql.shuffle.partitions", 3)

    val df = session.read
      .format("json")
      .load("data/flight-data/json/2015-summary.json")
  }
}
