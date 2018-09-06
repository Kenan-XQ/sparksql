package com.xq.spark.log.dao

import java.sql.{Connection, PreparedStatement}

import com.xq.spark.log.MySQLUtils
import com.xq.spark.log.po.{DayJobAccessStat, DayJobPersonMoneyStat}

import scala.collection.mutable.ListBuffer

/**
  * 5)统计各个城市Dao操作
  */
object StatDao {

  /**
    * 批量保存 DayJobAccessStat 对象到数据库
    * @param list
    */
  def insertJobAccessTopN(list: ListBuffer[DayJobAccessStat]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()

      //因为是批量处理，所以把自动提交改成false，我们要设置成手动提交
      connection.setAutoCommit(false)

      val sql = "insert into day_job_access_topn_stat(city, city_count) values(?, ?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.city)
        pstmt.setLong(2, ele.cityCount)

        pstmt.addBatch()  //把每条pstmt添加到批次中
      }

      pstmt.executeBatch()  //执行批量处理
      connection.commit()  //手动提交

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }



  /**
    * 批量保存 DayJobPersonMoneyStat 对象到数据库
    * @param list
    */
  def insertJobMoneyAccessTopN(list: ListBuffer[DayJobPersonMoneyStat]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()

      //因为是批量处理，所以把自动提交改成false，我们要设置成手动提交
      connection.setAutoCommit(false)

      val sql = "insert into day_job_person_money_stat(city, average_person_money) values(?, ?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.city)
        pstmt.setDouble(2, ele.averagePersonMoney)

        pstmt.addBatch()  //把每条pstmt添加到批次中
      }

      pstmt.executeBatch()  //执行批量处理
      connection.commit()  //手动提交

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }
}
