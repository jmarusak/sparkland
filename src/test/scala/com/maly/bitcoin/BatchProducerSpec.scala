package com.maly.bitcoin

import org.apache.spark.sql._
import org.apache.spark.sql.test.SharedSparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class BatchProducerSpec
    extends AnyFunSuite
    with Matchers
    with SharedSparkSession {}
