package com.xq.spark.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 2)访问日志转换(输入 => 输出)工具类
  */
object AccessConvertUtil {

  //定义的是输出的字段
  val struct = StructType(
    Array(
      StructField("jobName", StringType),
      StructField("factoryName", StringType),
      StructField("lowerMoney", LongType),
      StructField("averageMoney", LongType),
      StructField("higherMoney", LongType),
      StructField("city", StringType)
    )
  )

  /**
    * 根据输入的每一行信息转换成输出的样式
    * @param log 输入的每一行记录信息
    */
  def parseLog(log: String) = {

    try {
      val splits = log.split("\t")

      val jobName = splits(0)
      val factoryName = splits(1)
      val lowerMoney = splits(2).toLong
      val higherMoney = splits(3).toLong
      val city = splits(4)
      val averageMoney = (lowerMoney + higherMoney) / 2

      //这个Row里面的字段要和struct中的字段对应上
      Row(jobName, factoryName, lowerMoney, averageMoney, higherMoney, city)
    } catch {
      case e: Exception => Row(0)
    }
  }
}
