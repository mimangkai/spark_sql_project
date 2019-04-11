package cn.njupt.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换（输入==>输出）工具类
  */
object AccessConvertUtils {
  //URL、cmsType（video/article）、cmsID（编号）、流量、ip、城市信息、访问时间、天
  val schema = StructType(Array(
    StructField("url", StringType),
    StructField("cmsType", StringType),
    StructField("cmsId", LongType),
    StructField("traffic", LongType),
    StructField("ip", StringType),
    StructField("city", StringType),
    StructField("time", StringType),
    StructField("day", StringType)
  ))

  /**
    * 根据输入的每一行信息转换成输出的样式
    *
    * @param log 输入的每一行记录信息
    */
  def parseLog(log: String) = {
    try {
      val splits = log.split("\t")
      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)
      val time = splits(0)

      val domain = "http://www.imooc.com/"
      val cmsTypeId = url.substring(url.indexOf(domain) + domain.length).split("/")
      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }
      val city = IpUtils.getCity(ip)
      val day = time.split(" ")(0).replaceAll("-", "")

      //这个row里面的字段要和struct中的字段对应上
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e:Exception => Row(0)
    }
  }

  def main(args: Array[String]): Unit = {
    val log = "2016-11-10 00:01:02\t-\t813\t183.162.52.7"
    val row = parseLog(log)
    println(row.toString())
  }
}
