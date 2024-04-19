package com.maly.bitcoin

import java.sql.Timestamp

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class StreamingProducerSpec
    extends AnyWordSpec
    with Matchers {
      
  val jsonRaw =
    """{"amount":"0.00172969","date":"1712675895","price":"68951","tid":"332782270","type":"0"}"""
      
  val jsonRawStringArray =
    """[{"amount":"0.00172969","date":"1712675895","price":"68951","tid":"332782270","type":"0"},
       |{"amount":"0.00172969","date":"1712675895","price":"68951","tid":"332782270","type":"1"}]""".stripMargin
      
  val jsonListOfStrings =
    List("""{"amount":"0.00172969","date":"1712675895","price":"68951","tid":"332782270","type":"0"}""",
       """{"amount":"0.00172969","date":"1712675895","price":"68951","tid":"332782270","type":"1"}""")

  val rawTransaction = RawTransaction(
        "0.00172969", "1712675895", "68951", "332782270","0")
  
  val domainTransaction = Transaction(
    new Timestamp(1712675895000L), 332782270, 68951.0, false, 0.00172969)  

  val jsonDomain = 
    """{"timestamp":"2024-04-09 15:18:15","date":"2024-04-09","tid":332782270,"price":68951.0,"sell":false,"amount":0.00172969}"""

  "StreamingProducer.jsonStringArrayToListOfStrings" should {
    "create a RawTransaction object from a Json string" in {
      val jsonList: List[String] = StreamingProducer.jsonStringArrayToListOfStrings(jsonRawStringArray)
      jsonList should equal (jsonListOfStrings)
    }
  }

  "StreamingProducer.jsonToRawTransaction" should {
    "create a RawTransaction object from a Json string" in {
      val transaction: RawTransaction = StreamingProducer.jsonToRawTransaction(jsonRaw)
      transaction should equal (rawTransaction)
    }
  }

  "StreamingProducer.rawToDomainTransaction" should {
    "Convert RawTransaction to domain Transaction" in {
    val transaction: Transaction = StreamingProducer.rawToDomainTransaction(rawTransaction)
    transaction should equal (domainTransaction)
    }
  }

  "StreamingProducer.serializeTransaction" should {
    "Convert Transaction to Json string" in {
    val jsonTxn: String = StreamingProducer.serializeTransaction(domainTransaction)
    jsonTxn should equal (jsonDomain) 
    }
  }
}
