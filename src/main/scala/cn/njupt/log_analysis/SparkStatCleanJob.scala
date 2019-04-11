package cn.njupt.log_analysis

import cn.njupt.utils.{AccessConvertUtils, LoggerLevels}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 利用spark完成数据清洗操作
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    //读取初步清洗后的日志文件：time url traffic ip
    val file: RDD[String] = spark.sparkContext.textFile("E:\\IdeaProjects\\bigdata\\sparksqlimooclog\\src\\main\\resources\\imooc_log\\format.log")
    //file.take(10).foreach(println)

    //RDD ==> DataFrame
    val logDF = spark.createDataFrame(file.map(line => AccessConvertUtils.parseLog(line)),AccessConvertUtils.schema)
    //logDF.printSchema()
    //logDF.show(false)
    //数据清洗存储到目标地址
    logDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("E:\\IdeaProjects\\bigdata\\sparksqlimooclog\\src\\main\\output\\clean")

    spark.close()
  }
}
