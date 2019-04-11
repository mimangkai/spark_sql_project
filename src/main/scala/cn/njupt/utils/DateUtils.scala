package cn.njupt.utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间解析工具类
  * 日期格式转换：[dd/MMM/yyyy:HH:mm:ss Z] ==> yyyy-MM-dd HH:mm:ss
  * 注意：SimpleDateFormat是线程不安全的
  */
object DateUtils {
  //输入文件日期时间格式,必须加上语言环境Locale
  val INPUT_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  //目标时间格式
  val TARGET_TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 转换成yyyy-MM-dd HH:mm:ss
    *
    * @param time
    * @return
    */
  def parse(time: String) = {
    TARGET_TIME_FORMAT.format(getTime(time))
  }

  /**
    * @param time :String类型的时间：[10/Nov/2016:00:01:02 +0800]
    * @return Long的时间戳
    */
  def getTime(time: String) = {
    try {
      INPUT_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

  //测试
  def main(args: Array[String]): Unit = {
    val time = "[10/Nov/2016:00:01:02 +0800]"
    println(DateUtils.parse(time))
  }
}
