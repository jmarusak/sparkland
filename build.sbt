name := "sparkland"
version := "0.1"
scalaVersion := "2.12.18"
fork := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"
libraryDependencies += "io.delta" %% "delta-core" % "2.4.0"
