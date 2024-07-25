import java.time.Instant
import scala.concurrent.duration._

/*
import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types._
*/


val DurationProcessRun: FiniteDuration = 30.seconds
val DurationBatchCycle: FiniteDuration = 5.seconds
val DurationBackfill: FiniteDuration = 2.hours 

def processOneBatch(startTime: Instant, endTime: Instant): (Instant, Instant) = {
  Thread.sleep(DurationBatchCycle.toMillis)
  (endTime, Instant.now)
}

var timeNow = Instant.now
var (startTime, endTime) = processOneBatch(timeNow.minusSeconds(DurationBackfill.toSeconds), timeNow) 

val timeProcessEnd = timeNow.plusSeconds(DurationProcessRun.toSeconds) 
while (endTime.compareTo(timeProcessEnd) < 0) {
  println(s"$startTime  $endTime")
  val (startTimeNext, endTimeNext) = processOneBatch(startTime, endTime) 
  startTime = startTimeNext
  endTime = endTimeNext
}
