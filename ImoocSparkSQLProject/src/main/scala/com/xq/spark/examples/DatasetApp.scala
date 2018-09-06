package com.xq.spark.examples

import org.apache.spark.sql.SparkSession

/**
  * Dataset操作
  */
object DatasetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DatasetApp")
        .master("local[2]").getOrCreate()

    //注意：需要导入隐式转换
    import spark.implicits._


    val path = "file:///D:/sales.txt"

    //spark如何解析csv(txt)文件
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df.show()

    val ds = df.as[Sales]
    ds.map(line => line.itemId).show()

    spark.stop()

  }

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)
}
