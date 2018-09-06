package com.xq.spark.examples

import org.apache.spark.sql.SparkSession

/**
  * SparkSession的使用
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkSessionApp").master("local[2]").getOrCreate()

    val people = spark.read.json("file:///D:/people.json")
    people.show()

    spark.stop()
  }
}
