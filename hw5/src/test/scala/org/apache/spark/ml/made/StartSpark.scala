package org.apache.spark.ml.made

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec._
import org.scalatest.matchers._

class StartSpark extends AnyFlatSpec with should.Matchers {
  "Spark" should "start context" in {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[4]")
      .getOrCreate()

    Thread.sleep(60000)
  }
}
