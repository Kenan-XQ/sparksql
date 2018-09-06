package com.xq.spark.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成数据清洗操作：运行在Yarn之上
  */
object SparkStatCleanJobYarn {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: SparkStatCleanJobYarn <inputPath> <outputPath>")
      System.exit(1)
    }

    val Array(inputPath, outputPath) = args

    val spark = SparkSession.builder().getOrCreate()

    val accessRDD = spark.sparkContext.textFile(inputPath)

    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)


    accessDF.coalesce(1).write.format("parquet")
      .mode(SaveMode.Overwrite).save(outputPath)

    spark.stop()
  }
}
