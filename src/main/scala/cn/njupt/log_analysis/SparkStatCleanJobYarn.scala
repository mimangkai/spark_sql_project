package cn.njupt.log_analysis

import cn.njupt.utils.{AccessConvertUtils, LoggerLevels}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 利用spark完成数据清洗操作:运行在YARN上
  */
object SparkStatCleanJobYarn {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage:SparkStatCleanJobYarn <inputPath> <outPath>")
      System.exit(1)
    }

    val Array(inputPath, outPath) = args

    val spark = SparkSession.builder().getOrCreate()

    //读取初步清洗后的日志文件：time url traffic ip
    val file: RDD[String] = spark.sparkContext.textFile(inputPath)
    //file.take(10).foreach(println)

    //RDD ==> DataFrame
    val logDF = spark.createDataFrame(file.map(line => AccessConvertUtils.parseLog(line)), AccessConvertUtils.schema)
    //logDF.printSchema()
    //logDF.show(false)
    //数据清洗存储到目标地址
    logDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save(outPath)

    spark.close()
  }
}
