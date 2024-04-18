package com.maly.bitcoin

import java.sql.Timestamp

import org.apache.spark.sql.{SparkSession, Dataset}

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class BatchProducerSpec
    extends AnyWordSpec
    with Matchers 
    with BeforeAndAfterAll {

  var sparkSession: SparkSession =_
  
  override def beforeAll(): Unit = {
    sparkSession = SparkSession
      .builder()
      .appName("TestSuite")
      .master("local[1]")
      .getOrCreate()
  }
  
  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  val rawTransaction1 = RawTransaction(
        "0.00172969", "1712675895", "68951", "332782270","0")
  val rawTransaction2 = RawTransaction(
        "0.00707741", "1712675892", "68948", "332782264","0")
  
  val domainTransaction1 = Transaction(
    new Timestamp(1712675895), 332782270, 68951.0, false, 0.00172969)  
  val domainTransaction2 = Transaction(
    new Timestamp(1712675892), 332782264, 68948.0, false, 0.00707741)  

  "BatchProducer.jsonToRawTransaction" should {
    "create a Dataset[RawTransaction] from a Json string" in {
      implicit val spark: SparkSession = sparkSession
      
      val json =
        """[{"amount":"0.00172969","date":"1712675895","price":"68951","tid":"332782270","type":"0"},
           |{"amount":"0.00707741","date":"1712675892","price":"68948","tid":"332782264","type":"0"}]""".stripMargin

      val ds: Dataset[RawTransaction] = BatchProducer.jsonToRawTransaction(json)
      ds.collect() should contain theSameElementsAs Seq(rawTransaction1, rawTransaction2)
    }
  }
}
