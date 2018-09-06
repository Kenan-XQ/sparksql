package com.xq.spark.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 2)使用Spark完成我们的数据清洗操作
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {

    //因为主机没有配置hadoop环境，所以需要加上这句话
    System.setProperty("hadoop.home.dir", "E:/winutils/")

    val spark = SparkSession.builder().appName("SparkStatCleanJob")
        .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///D:/output/part-00000")
    //accessRDD.take(10).foreach(println)

    //RDD => DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    //accessDF.printSchema()
    //accessDF.show(true)

    //按照"city"进行分区 然后把数据文件放到D:/clean下，coalesce(1): 每个分区下只有一个文件
    //accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
    //  .partitionBy("city").save("file:///D:/clean/")

    //这里出错了！！！！输出到parquet文件中文乱码，但是可以写进数据库是中文的
    accessDF.coalesce(1).write.format("parquet")
      .mode(SaveMode.Overwrite).save("file:///D:/clean/")

    spark.stop()
  }
}
