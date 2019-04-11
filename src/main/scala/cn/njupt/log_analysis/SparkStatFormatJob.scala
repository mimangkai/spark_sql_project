package cn.njupt.log_analysis

import cn.njupt.utils.{DateUtils, LoggerLevels}
import org.apache.spark.sql.SparkSession

/**
  * 原始日志初步清洗—抽取需要的指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()
    //读取原始日志
    val file = spark.sparkContext.textFile("E:\\IdeaProjects\\bigdata\\sparksqlimooclog\\src\\main\\resources\\imooc_log\\mooc_access.log")

    //从日志中抽取指定列的数据，并做格式转换
    file.filter(line=>{
      line.split(" ")(11).contains("video")||line.split(" ")(11).contains("article")
    }).map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      /**
        * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间：
        * [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
        */
      val visit_time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"", "")
      val traffic = splits(9)
      DateUtils.parse(visit_time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("E:\\IdeaProjects\\bigdata\\sparksqlimooclog\\src\\main\\output")

    spark.close()
  }
}
