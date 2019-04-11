package cn.njupt.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * mysql操作工具类
  */
object MySQLUtils {
  val url = "jdbc:mysql://localhost:3306/imooc_project"
  val username = "root"
  val password = "123456"

  //获取数据库连接
  def getConnection() = {
    classOf[com.mysql.jdbc.Driver]
    DriverManager.getConnection(url,username,password)
  }

  /**
    * 释放数据库连接等资源
    * @param conn
    * @param pstmt
    */
  def release(conn: Connection, pstmt: PreparedStatement) = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

    def main(args: Array[String]): Unit = {
      println(getConnection())
    }
}
