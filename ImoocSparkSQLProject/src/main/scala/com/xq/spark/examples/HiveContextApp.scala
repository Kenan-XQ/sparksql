package com.xq.spark.examples

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HiveContext的使用
  * 使用时需要通过--jars 把mysql的驱动传递给classpath
  */
object HiveContextApp {

  def main(args: Array[String]): Unit = {
    //1.创建相应的Context
    val sparkConf = new SparkConf()

    //在测试或者生产中，AppName和Master我们是通过脚本进行指定的
    //sparkConf.setAppName("HiveContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2.相关的处理
    hiveContext.table("hive_wordcount").show

    //3.关闭资源
    sc.stop()
  }
}
