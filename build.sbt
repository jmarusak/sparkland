name := "sparkland"
version := "1.0"
scalaVersion := "2.12.18"
fork := true

val sparkVersion = "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.7.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test

//Compile / mainClass := Some("com.maly.bitcoin.StreamingProducerApp")
