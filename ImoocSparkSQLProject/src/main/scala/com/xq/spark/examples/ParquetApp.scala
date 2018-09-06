package com.xq.spark.examples

import org.apache.spark.sql.SparkSession

/**
  * Parquet文件操作
  */
object ParquetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetApp")
      .master("local[2]").getOrCreate()

    /**
      * spark.read.format("parquet").load 这是标准写法
      */
    val userDF = spark.read.format("parquet").load("file:///home/Kiku/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")
    userDF.printSchema()
    userDF.show()

    userDF.select("name", "favorite_color").show()
    //把读出来的两列 转换成json写入新文件中
    userDF.select("name", "favorite_color").write.format("json").save("file:///home/Kiku/data/jsonout")


    spark.read.load("file:///home/Kiku/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").show()

    //会报错，因为sparksql默认处理的format就是parquet
    spark.read.load("file:///home/Kiku/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").show()

    spark.read.format("parquet").option("path", "file:///home/Kiku/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").load().show()

    //调整默认分区大小 默认是200
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "10")

    //操作mysql 的数据
    spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sparksql")
        .option("dbtable", "sparksql.TBLS").option("user", "root")
        .option("password", "MySQLKenan_07").option("driver", "com.mysql.jdbc.Driver").load()

    spark.stop()
  }

}
