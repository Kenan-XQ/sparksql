package com.xq.spark.examples

import org.apache.spark.sql.SparkSession

/**
  * DataFrame中的操作，综合实例
  */
object DataFrameCase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    //RDD => DataFrame
    val rdd = spark.sparkContext.textFile("file:///D:/student.txt")

    //注意：需要导入隐式转换
    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    //最多显示30行数据，false：表示长的数据不会被截取掉
    studentDF.show(30, false)

    //取出前10条记录
    studentDF.take(10)
    studentDF.first()
    studentDF.head(3)

    //截取其中的几列
    studentDF.select("email").show(30, false)

    studentDF.filter("name='' OR name='NULL'").show()

    //找出name以M开头的人
    studentDF.filter("SUBSTR(name, 0, 1)='M'").show()

    //按照name进行排序
    studentDF.sort(studentDF.col("name")).show()
    studentDF.sort(studentDF.col("name").desc).show()

    studentDF.sort("name", "id").show()
    studentDF.sort(studentDF.col("name").asc, studentDF.col("id").desc).show()

    studentDF.select(studentDF.col("name").as("student_name")).show()

    //join
    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    studentDF.join(studentDF2, studentDF.col("id") === studentDF2.col("id")).show()

    spark.stop()

  }

  //模式类：实例化对象的时候可以不用new，默认public，有默认的toString方法，不能被继承
  case class Student(id: Int, name: String, phone: String, email: String)

}
