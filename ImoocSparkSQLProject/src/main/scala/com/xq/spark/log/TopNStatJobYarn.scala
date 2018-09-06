package com.xq.spark.log

import com.xq.spark.log.dao.StatDao
import com.xq.spark.log.po.{DayJobAccessStat, DayJobPersonMoneyStat}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, sum}

import scala.collection.mutable.ListBuffer

/**
  * 统计作业运行在Yarn之上
  */
object TopNStatJobYarn {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("Usage: TopNStatJobYarn <inputPath>")
      System.exit(1)
    }

    val Array(inputPath) = args

    val spark = SparkSession.builder().getOrCreate()

    val accessDF = spark.read.format("parquet").load(inputPath)

    accessDF.printSchema()
    accessDF.show()


    //所有城市中大数据工作岗位的数量 TOP 10
    jobAccessTopNStat(spark, accessDF)

    //所有城市中 大数据职位 的平均收入 TOP 10
    jobMoneyAccessTopNStat(spark, accessDF)


    spark.stop()
  }


  /**
    * 所有城市中 大数据职位 的平均收入 TOP 10
    * @param spark
    * @param accessDF
    */
  def jobMoneyAccessTopNStat(spark: SparkSession, accessDF: DataFrame) = {

    import spark.implicits._
    val jobMoneyAccessTopNDF = accessDF.filter($"averageMoney" > 0).groupBy("city")
      .agg((sum("averageMoney")/count("city")).as("averagePersonMoney"))
      .filter(count("city") > 100).orderBy($"averagePersonMoney".desc).limit(10)
    //jobMoneyAccessTopNDF.show()

    /**
      * 将统计结果写入mysql中
      */
    try {
      jobMoneyAccessTopNDF.foreachPartition(partitionOfReccords => {
        val list = new ListBuffer[DayJobPersonMoneyStat]

        partitionOfReccords.foreach(info => {
          val city = info.getAs[String]("city")
          val averagePersonMoney = info.getAs[Double]("averagePersonMoney")

          list.append(DayJobPersonMoneyStat(city, averagePersonMoney))
        })

        StatDao.insertJobMoneyAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  /**
    * 所有城市中大数据工作岗位的数量 TOP 10
    * @param spark
    * @param accessDF
    */
  def jobAccessTopNStat(spark: SparkSession, accessDF: DataFrame) = {

    //FIXME DataFrame API 的方式
    import spark.implicits._
    //accessDF.filter($"city" === "北京" && $"averageMoney" > 0)
    //agg: 聚合函数.
    //如果结果想增加列信息，在groupBy 后面增加列名 "city","jobName"....
    val jobAccessTopNCountDF = accessDF.filter($"averageMoney" > 0).groupBy("city")
      .agg(count("city").as("city_count")).orderBy($"city_count".desc).limit(10)

    //默认取出前20条数据
    //jobAccessTopNCountDF.show(false)

    /**
      * 将统计结果写入mysql中
      */
    try {
      jobAccessTopNCountDF.foreachPartition(partitionOfReccords => {
        val list = new ListBuffer[DayJobAccessStat]

        partitionOfReccords.foreach(info => {
          val city = info.getAs[String]("city")
          val cityCount = info.getAs[Long]("city_count")

          list.append(DayJobAccessStat(city, cityCount))
        })

        StatDao.insertJobAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }


    //FIXME SQL 方式
    //    accessDF.createOrReplaceTempView("access_logs")
    //    val jobAccessTopNCountDF = spark.sql("select city, count(1) as city_count from access_logs " +
    //      "where averageMoney > 0  group by city " +
    //      "order by city_count desc")
    //
    //    jobAccessTopNCountDF.show()
  }

}
