## 基于spark sql统计网站访问量
**项目简介：** \
使用spark对原始日志进行数据清洗和数据处理，通过DataFrame处理得到各个指标的统计表，并持久化到MySQL数据库中，最后使用百度的开源数据可视化工具ECharts将统计表的数据以折线图、柱形图等形式展现到页面上，以供商业分析和决策，最后将作业提交到spark on yarn上运行。

**项目需求** \
需求一：按天统计imooc主站最受欢迎的课程/手记的Top N访问次数  
需求二：按地市统计imooc主站最受欢迎的TopN课程 \
需求三：按流量统计imooc主站最受欢迎的TopN课程

**项目环境搭建：**
spark、mysql

### 功能实现-数据清洗
原始日志解析： \
提取（访问时间、访问URL、耗费的流量、访问IP地址信息）
```scala
//读取原始日志
val file = spark.sparkContext.textFile("E:\\IdeaProjects\\bigdata\\sparksqlimooclog\\src\\main\\resources\\imooc_log\\mooc_access.log")

//从日志中抽取指定列的数据，并做格式转换
file.map(line => {
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
```
二次清洗 \
使用spark SQL解析访问日志
解析出课程标号、类型
根据IP解析出城市信息
使用Spark SQL将访问时间按天进行分区输出
一般的日志处理方式，需要进行分区 
按照日志中的访问时间进行分区，比如：d,h,m5（每5分钟一个分区）

输入：访问时间、访问URL、耗费的流量、访问IP地址信息 
输出：URL、cmsType（video/article）、cmsID（编号）、流量、ip、城市信息、访问时间、天
```scala
//读取初步清洗后的日志文件：time url traffic ip
val file: RDD[String] = spark.sparkContext.textFile("E:\\IdeaProjects\\bigdata\\sparksqlimooclog\\src\\main\\resources\\imooc_log\\format.log")

//RDD ==> DataFrame
val logDF = spark.createDataFrame(file.map(line => AccessConvertUtil.parseLog(line)),AccessConvertUtil.schema)

//数据清洗存储到目标地址
logDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("E:\\IdeaProjects\\bigdata\\sparksqlimooclog\\src\\main\\output\\clean")
```

使用github上已有的开源项目—ipdatabase（根据ip找到城市） 
1) `git clone https://github.com/wzhe06/ipdatabase.git`
2) 编译下载的项目 `mvn clean package -DskipTests` 
3) 安装jar包到自己的maven仓库 
`mvn install:install-file -Dfile= .jar -DgroupId=com.ggstar -DartifactId=ipdatabase -Dversion=1.0 -Dpackaging=jar`
4) 将包依赖导入pom文件，并将源码resources里的IP地址信息文件复制到项目的resources路径 
使用放法：
```scala
def getCity(ip: String) = {
  IpHelper.findRegionByIp(ip)
}
```

### 功能实现—统计功能
按照需求完成统计信息并将统计结果入库

* 使用DataFrame API完成统计分析
* 使用SQL API完成统计分析
* 将统计分析结果写入到MySQl数据库

需求一：统计imooc主站最后欢迎的课程/手记的TopN访问次数 
步骤一：利用DataFrame的API进行统计分析 \
见代码cn.njupt.log_analysis.TopNStatJob \
步骤二：根据统计表设计对应实体类 \
`case class DayVideoAccessTopN(day:String,cmsId:Long,times:Long)` \
步骤三：创建数据库imooc_project及数据表day_video_access_topn_stat:
```sql
create database imooc_project;
use imooc_project;
create table day_video_access_topn_stat(
    day varchar(10) not null,
    cms_id bigint(10) not null,
    times bigint(10) not null,
    primary key(day,cms_id)
);
```
步骤四：将DataFrame得到的统计表写到MySQL数据表（分区导入，减少连接和释放资源的次数）
见代码cn.njupt.log_analysis.TopNStatJob

需求二：按地市统计imooc主站最受欢迎的TopN课程 
DataFrame得到统计表
见代码cn.njupt.log_analysis.TopNStatJob
创建数据表day_city_video_access_topn_stat：
```sql
use imooc_project;
create table day_city_video_access_topn_stat(
    day varchar(10) not null,
    city varchar(20) not null,
    cms_id bigint(10) not null,
    times bigint(10) not null,
    times_rank int not null,
    primary key(day,city,cms_id)
);
```
需求三：按流量统计imooc主站最受欢迎的TopN课程 
DataFrame得到统计表
见代码cn.njupt.log_analysis.TopNStatJob

创建数据表day_video_traffics_topn_stat:
```sql
create table day_video_traffics_topn_stat(
    day varchar(10) not null,
    cms_id bigint(10) not null,
    traffics bigint(10) not null,
    primary key(day,cms_id)
);
```