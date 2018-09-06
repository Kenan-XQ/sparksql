package com.xq.spark.log

import org.apache.spark.sql.SparkSession

/**
  * 1)第一步清洗：抽取出我们所需要的指定列的数据
  */
object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {

    //因为主机没有配置hadoop环境，所以需要加上这句话
    System.setProperty("hadoop.home.dir", "E:/winutils/")

    val spark = SparkSession.builder().appName("SparkStatFormatJob")
        .master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///D:/recruitData.txt")
    //access.take(10).foreach(println)

    access.map(line => {
      val splits = line.split("\t")

      val jobName = splits(0)
      val factoryName = splits(1).replaceAll(" ", "")
      var lowerMoney = ""
      var higherMoney = ""
      if (splits(2).indexOf("-") > 0) {
        lowerMoney = splits(2).substring(0, splits(2).indexOf("-"))
        higherMoney = splits(2).substring(splits(2).indexOf("-") + 1, splits(2).indexOf("元"))
      } else {
        lowerMoney = "0"
        higherMoney = "0"
      }
      val city = splits(3)
      //(jobName, factoryName, lowerMoney, higherMoney, city)
      jobName + "\t" + factoryName + "\t" + lowerMoney + "\t" + higherMoney + "\t" + city
    }).saveAsTextFile("file:///D:/output/")

    spark.stop()
  }
}
