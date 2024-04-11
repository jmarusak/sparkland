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

  val httpTransaction1 = HttpTransaction(
        "0.00172969", "1712675895", "68951", "332782270","0")
  val httpTransaction2 = HttpTransaction(
        "0.00707741", "1712675892", "68948", "332782264","0")
  
  val domainTransaction1 = Transaction(
    new Timestamp(1712675895), 332782270, 68951.0, false, 0.00172969)  
  val domainTransaction2 = Transaction(
    new Timestamp(1712675892), 332782264, 68948.0, false, 0.00707741)  

  "BatchProducer.jsonToHttpTransaction" should {
    "create a Dataset[HttpTransaction] from a Json string" in {
      implicit val spark: SparkSession = sparkSession
      
      val json =
        """[{"amount":"0.00172969","date":"1712675895","price":"68951","tid":"332782270","type":"0"},
           |{"amount":"0.00707741","date":"1712675892","price":"68948","tid":"332782264","type":"0"}]""".stripMargin

      val ds: Dataset[HttpTransaction] = BatchProducer.jsonToHttpTransaction(json)
      ds.collect() should contain theSameElementsAs Seq(httpTransaction1, httpTransaction2)
    }
  }
  
  "BatchProducer.unsafeSave" should {
    "saves a Dataset[Transaction] to parquet file" in {
      implicit val spark: SparkSession = sparkSession
      import spark.implicits._

      val path = new java.net.URI("./data/bitcoin")
      val sourceDS: Dataset[Transaction] = Seq(domainTransaction1, domainTransaction2).toDS()

      BatchProducer.unsafeSave(sourceDS, path)

      val readDS = spark.read.parquet(path.toString).as[Transaction]
      sourceDS.collect() should contain theSameElementsAs readDS.collect()
    }
  }
}
