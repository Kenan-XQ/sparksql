package com.xq.spark.examples

import org.apache.spark.sql.SparkSession

/**
  * 使用外部数据源综合查询Hive和MySQL的表数据
  */
object HiveMySQLApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("HiveMySQLApp")
        .master("local[2]").getOrCreate()

    //加载Hive表的数据
    val hiveDF = spark.table("emp")

    //加载mysql的数据
    val mysqlDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sparksql").option("dbtable", "sparksql.DEPT").option("user", "root").option("password", "MySQLKenan_07").option("driver", "com.mysql.jdbc.Driver").load()

    //JOIN
    val resultDF = hiveDF.join(mysqlDF, hiveDF.col("deptno") === mysqlDF.col("DEPTNO"))

    resultDF.select(hiveDF.col("empno"), hiveDF.col("ename"),
      mysqlDF.col("deptno"), mysqlDF.col("dname")).show()

    resultDF.show()

    spark.stop()
  }
}
