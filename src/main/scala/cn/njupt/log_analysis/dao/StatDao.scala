package cn.njupt.log_analysis.dao

import java.sql.{Connection, PreparedStatement}

import cn.njupt.log_analysis.domain.{DayCityVideoAccessTopN, DayVideoAccessTopN, DayVideoTrafficsTopN}
import cn.njupt.utils.MySQLUtils

import scala.collection.mutable.ListBuffer

object StatDao {
  /**
    * 批量保存DayVideoTrafficsTopN到数据库
    *
    * @param list
    */
  def insertDayVideoTrafficsTopN(list: ListBuffer[DayVideoTrafficsTopN]): Unit = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection()
      conn.setAutoCommit(false) //设置为手动提交

      val sql = "insert into day_video_traffics_topn_stat(day,cms_id,traffics) values (?,?,?)"
      pstmt = conn.prepareStatement(sql)
      list.foreach(ele => {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)
        pstmt.addBatch()
      })
      pstmt.executeBatch() //执行批量处理
      conn.commit() //手动提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn, pstmt)
    }
  }

  /**
    * 批量保存DayCityVideoAccessTopN到数据库
    *
    * @param list
    */
  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessTopN]): Unit = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection()
      conn.setAutoCommit(false) //设置为手动提交

      val sql = "insert into day_city_video_access_topn_stat(day,city,cms_id,times,times_rank) values (?,?,?,?,?)"
      pstmt = conn.prepareStatement(sql)
      list.foreach(ele => {
        pstmt.setString(1, ele.day)
        pstmt.setString(2, ele.city)
        pstmt.setLong(3, ele.cmsId)
        pstmt.setLong(4, ele.times)
        pstmt.setLong(5, ele.times_rank)
        pstmt.addBatch()
      })
      pstmt.executeBatch() //执行批量处理
      conn.commit() //手动提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn, pstmt)
    }
  }

  /**
    * 批量保存DayVideoAccessTopN到数据库
    *
    * @param list
    */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessTopN]) = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection()
      conn.setAutoCommit(false) //设置为手动提交

      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?)"
      pstmt = conn.prepareStatement(sql)
      list.foreach(ele => {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)
        pstmt.addBatch()
      })
      pstmt.executeBatch() //执行批量处理
      conn.commit() //手动提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn, pstmt)
    }
  }

  /**
    * 删除指定日期的数据
    *
    * @param day
    */
  def deleteSpecificDateData(day: String): Unit = {
    val tables = Array("day_video_access_topn_stat",
      "day_city_video_access_topn_stat",
      "day_video_traffics_topn_stat")

    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection()
      for (table <- tables) {
        val sql = s"delete from $table where day=?"
        pstmt = conn.prepareStatement(sql)
        pstmt.setString(1,day)
        pstmt.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn, pstmt)
    }
  }
}
