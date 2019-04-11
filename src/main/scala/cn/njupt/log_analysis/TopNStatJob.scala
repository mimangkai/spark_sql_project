package cn.njupt.log_analysis

import cn.njupt.log_analysis.dao.StatDao
import cn.njupt.log_analysis.domain.{DayCityVideoAccessTopN, DayVideoAccessTopN, DayVideoTrafficsTopN}
import cn.njupt.utils.LoggerLevels
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * TopN统计Spark作业
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
      .master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("E:\\IdeaProjects\\bigdata\\sparksqlimooclog\\src\\main\\output\\clean")
    accessDF.printSchema()
    accessDF.show(false)

    val day = "20170511"

    import spark.implicits._
    val commonDF = accessDF.filter($"day" === day && $"cmsType" === "video")

    commonDF.cache() //添加缓存
    //删除指定日期的数据
    StatDao.deleteSpecificDateData(day)
    //最受欢迎的TopN课程
    videoAccessTopNStat(spark, commonDF)
    //按照地市统计TopN课程
    cityVideoAccessTopNStat(spark, commonDF)
    //按照流量统计TopN课程
    dayVideoTrafficsTopNStat(spark, commonDF)
    commonDF.unpersist() //删除缓存

    spark.close()
  }

  /**
    * 按照流量统计TopN课程
    *
    * @param spark
    * @param accessDF
    * @param day
    */
  def dayVideoTrafficsTopNStat(spark: SparkSession, commonDF: DataFrame) = {
    //使用DataFrame方式统计TopN课程
    import spark.implicits._
    val dayVideoTrafficsTopNDF = commonDF
      .groupBy($"day", $"cmsId").agg(sum("traffic") as "traffics").orderBy($"traffics".desc)
    //dayVideoTrafficsTopNDF.show

    //将统计结果写入到MySQL
    try {
      dayVideoTrafficsTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsTopN]
        partitionOfRecords.foreach(
          record => {
            val day = record.getAs[String]("day")
            val cmsId = record.getAs[Long]("cmsId")
            val traffics = record.getAs[Long]("traffics")

            /**
              * 不建议在此处进行数据库的数据插入，分离到dao进行批量插入
              */
            list.append(DayVideoTrafficsTopN(day, cmsId, traffics))
          }
        )
        StatDao.insertDayVideoTrafficsTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 按照地市统计TopN课程
    *
    * @param spark
    * @param accessDF
    * @param day
    */
  def cityVideoAccessTopNStat(spark: SparkSession, commonDF: DataFrame) = {
    //使用DataFrame方式统计TopN课程
    import spark.implicits._
    val cityVideoAccessTopNDF = commonDF
      .groupBy("day", "city", "cmsId").agg(count("cmsId") as "times").orderBy($"times".desc)

    val top3DF = cityVideoAccessTopNDF.select($"day", $"city", $"cmsId", $"times",
      row_number().over(Window.partitionBy("city").orderBy($"times".desc)) as "times_rank").filter("times_rank <= 3")
    //top3DF.show()

    //将统计结果写入到MySQL
    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessTopN]
        partitionOfRecords.foreach(
          record => {
            val day = record.getAs[String]("day")
            val city = record.getAs[String]("city")
            val cmsId = record.getAs[Long]("cmsId")
            val times = record.getAs[Long]("times")
            val times_rank = record.getAs[Int]("times_rank")

            list.append(DayCityVideoAccessTopN(day, city, cmsId, times, times_rank))
          }
        )
        StatDao.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 最受欢迎的TopN课程
    *
    * @param spark
    * @param accessDF
    * @param day
    */
  def videoAccessTopNStat(spark: SparkSession, commonDF: DataFrame) = {
    //使用DataFrame方式统计TopN课程
    import spark.implicits._
    val videoAccessTopNDF = commonDF
      .groupBy($"day", $"cmsId").agg(count("cmsId") as "times").orderBy($"times".desc)

    //使用sql方式统计TopN课程
    //    accessDF.createOrReplaceTempView("t_access_log")
    //    val videoAccessTopNDF = spark.sql(s"select day,cmsId,count(1) as times from t_access_log " +
    //      s"where day=$day and cmsType='video' group by day,cmsId order by times desc")

    //videoAccessTopNDF.show()

    //将统计结果写入到MySQL
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessTopN]
        partitionOfRecords.foreach(
          record => {
            val day = record.getAs[String]("day")
            val cmsId = record.getAs[Long]("cmsId")
            val times = record.getAs[Long]("times")

            /**
              * 不建议在此处进行数据库的数据插入，分离到dao进行批量插入
              */
            list.append(DayVideoAccessTopN(day, cmsId, times))
          }
        )
        StatDao.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
