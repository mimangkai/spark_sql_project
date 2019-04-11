package cn.njupt.utils

import com.ggstar.util.ip.IpHelper

/**
  * IP解析工具类
  */
object IpUtils {
  def getCity(ip: String) = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    val city = getCity("58.30.15.255")
    println(city)
  }
}
