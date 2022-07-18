package cn.covid.process

import cn.covid.bean.{CovidBean, StatisticsDataBean}
import cn.covid.util.BaseJdbcSink
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.{StringOps, mutable}
/**
 * Author：
 * Date：2022/6/1015:24
 * Desc:
 */
object Covid19_Data_Process {
  def main(args: Array[String]): Unit = {
    // 1.创建StructuredStreaming执行环境

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Covid19_Data_Process").getOrCreate()
    val sc:SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    import org.apache.spark.sql.functions._
    import spark.implicits._
    import scala.collection.JavaConversions._



    // 2. 连接Kafka
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.29.129:9091,192.168.29.129:9092,192.168.29.129:9093")
      .option("subscribe", "covid19")
      .load()

    // 取出消息中的value
    val jsonStrDS:Dataset[String] = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
    jsonStrDS.writeStream
      .format("console")  // 输出目的地
      .outputMode("append") // 输出模式
      .trigger(Trigger.ProcessingTime(0))//触发间隔，0表示尽可能快的执行
      .option("truncate",false) //表示列名如果过长不进行截断
      .start()
      .awaitTermination()

    // 3. 处理数据
    // 解析Dataset[json字符串]===>Dataset[DeviceData]
    val covidBeanDS: Dataset[CovidBean] = jsonStrDS.map(jsonStrDS => {
      JSON.parseObject(jsonStrDS, classOf[CovidBean])
    })
    // 分离出省份数据
    val provinceDS:Dataset[CovidBean] = covidBeanDS.filter(_.statisticsData != null)
    // 分离出城市数据
    val cityDS:Dataset[CovidBean] = covidBeanDS.filter(_.statisticsData == null)
    // 分离出每个省份每一天的统计数据
    val statisticsDataDS: Dataset[StatisticsDataBean] = provinceDS.flatMap(p => {
      val jsonStr: StringOps = p.statisticsData //获取到的该数据是该省份的统计数据组成的jsonStr数据
      val list: mutable.Buffer[StatisticsDataBean] = JSON.parseArray(jsonStr, classOf[StatisticsDataBean])
      list.map(s => {
        s.provinceShortName = p.provinceShortName
        s.locationId = p.locationId
        s
      })
    })
    // 4. 统计分析
    // 全国疫情汇总信息:现有确诊,累计确诊,现有疑似,累计治愈,累计死亡--按照日期聚合汇总入库覆盖--省份数据
    val result1: DataFrame = provinceDS.groupBy(cols = Symbol("datetime"))
      .agg(
        sum(Symbol("currentConfirmedCount")) as "currentConfirmedCount", //现有确诊
        sum(Symbol("confirmedCount")) as "confirmedCount", //累计确诊
        sum(Symbol("suspectedCount")) as "suspectedCount", //现有疑似
        sum(Symbol("curedCount")) as "curedCount", //累计治愈
        sum(Symbol("deadCount")) as "deadCount", //累计死亡
      )

    // 全国各省份累计确诊数地图--按照日期-省份聚合汇总入库覆盖--省份数据
    val result2 : DataFrame= provinceDS.select(
      Symbol("datetime"), Symbol("locationId"), Symbol("provinceShortname"), Symbol("currentConfirmedCount"),
      Symbol("suspectedCount"), Symbol("curedCount"), Symbol("deadCount"))

    // 全国疫情新增趋势--按照日期聚合汇总新增确诊-入库覆盖--省份数据中的时间序列数据
    val result3: DataFrame = statisticsDataDS.groupBy(Symbol("dateId"))
      .agg(
        sum(Symbol("confirmedIncr")) as "confirmedIncr", //新增确诊
        sum(Symbol("confirmedCount")) as "confirmedCount", //累计确诊
        sum(Symbol("suspectedCount")) as "suspectedCount", //累计疑似
        sum(Symbol("curedCount")) as "curedCount", //累计治愈
        sum(Symbol("deadCount")) as "deadCount" //累计死亡
      )

    //境外输入TopN--按照城市聚合汇总排序入库覆盖--城市数据
    val result4: Dataset[Row] = cityDS.filter(_.cityName.equals("境外输入"))
      .groupBy(Symbol("datetime"), Symbol("provinceShortName"), Symbol("pid"))
      .agg(sum(Symbol("confirmedCount")) as "confirmedCount")
      .sort(Symbol("confirmedCount").desc)

    // 统计北京市的累计确诊地图
    val result5:DataFrame = cityDS.filter(_.provinceShortName.equals("北京"))
      .select(Symbol("datetime"), Symbol("locationId"), Symbol("provinceShortname"), Symbol("currentConfirmedCount"),
        Symbol("suspectedCount"), Symbol("curedCount"), Symbol("deadCount"))

    // 5. 结果输出
    /**
     * 输出模式有3种
     * append:默认的，表示只输出新增数据，支持简单查询不支持聚合
     * complete:表示完整模式，所有数据都会输出，必须包含聚合操作
     * update:表示更新模式，只输出有变化的数据，不支持排序
     * */
    result1.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    result1.writeStream
      .foreach(new BaseJdbcSink(sql = "REPLACE INTO `summary_info` " +
        "(`datetime`, `currentConfirmedCount`, `confirmedCount`, `suspectedCount`, `curedCount`, `deadCount`) " +
        "VALUES (?, ?, ?, ?, ?, ?);") {
        override def realProcess(sql: String, row: Row): Unit = {
          //注意row的索引从0开始
          val datetime: String = row.getAs[String]("datetime")
          val currentConfirmedCount: Long = row.getAs[Long]("currentConfirmedCount")
          val confirmedCount: Long = row.getAs[Long]("confirmedCount")
          val suspectedCount: Long = row.getAs[Long]("suspectedCount")
          val curedCount: Long = row.getAs[Long]("curedCount")
          val deadCount: Long = row.getAs[Long]("deadCount")
          //REPLACE表示如果有值则替换,如果没有值则新增(本质上是如果有值则删除再新增,如果没有值则直接新增)
          //注意:在创建表的时候需要指定唯一索引,或者联合主键来判断有没有这个值
          ps = conn.prepareStatement(sql)
          ps.setString(1,datetime)//jdbc的索引从1开始
          ps.setLong(2,currentConfirmedCount)
          ps.setLong(3,confirmedCount)
          ps.setLong(4,suspectedCount)
          ps.setLong(5,curedCount)
          ps.setLong(6,deadCount)
          ps.executeUpdate()
        }
      }).outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    // 将结果2输出到控制台
    result2.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    // 将结果2输出到MySQL数据库
    result2.writeStream
      // 在2.2.0版本只能使用foreach,效率较低
      // 在2.4版本有foreachBatch,效率较高
      .foreach(new BaseJdbcSink("REPLACE INTO `province_confirmed_count` (`datetime`, `locationId`, `provinceShortName`, `currentConfirmedCount`, `confirmedCount`, `suspectedCount`, `curedCount`, `deadCount`) VALUES (?, ?, ?, ?, ?, ?, ?, ?);") {
        override def realProcess(sql: String, row: Row): Unit = {//注意row的索引从0开始
          val datetime: String = row.getAs[String]("datetime")
          val locationId: Int = row.getAs[Int]("locationId")
          val provinceShortName: String = row.getAs[String]("provinceShortName")
          val currentConfirmedCount: Int = row.getAs[Int]("currentConfirmedCount")
          val confirmedCount: Int = row.getAs[Int]("confirmedCount")
          val suspectedCount: Int = row.getAs[Int]("suspectedCount")
          val curedCount: Int = row.getAs[Int]("curedCount")
          val deadCount: Int = row.getAs[Int]("deadCount")

          ps = conn.prepareStatement(sql)
          ps.setString(1,datetime)//jdbc的索引从1开始
          ps.setInt(2,locationId)
          ps.setString(3,provinceShortName)
          ps.setInt(4,currentConfirmedCount)
          ps.setInt(5,confirmedCount)
          ps.setInt(6,suspectedCount)
          ps.setInt(7,curedCount)
          ps.setInt(8,deadCount)
          ps.executeUpdate()
        }
      })
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    // result3 输出到控制台
    result3.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()


    // result3 输出到MySQL
    result3.writeStream
      .foreach(new BaseJdbcSink("REPLACE INTO `nation_confirmedIncr` (`dateId`, `confirmedIncr`, `confirmedCount`, `suspectedCount`, `curedCount`, `deadCount`) VALUES (?, ?, ?, ?, ?, ?);") {
        override def realProcess(sql: String, row: Row): Unit = {//注意row的索引从0开始
          val dateId: String = row.getAs[String]("dateId")
          val confirmedIncr: Long = row.getAs[Long]("confirmedIncr")
          val confirmedCount: Long = row.getAs[Long]("confirmedCount")
          val suspectedCount: Long = row.getAs[Long]("suspectedCount")
          val curedCount: Long = row.getAs[Long]("curedCount")
          val deadCount: Long = row.getAs[Long]("deadCount")
          ps = conn.prepareStatement(sql)
          ps.setString(1,dateId)//jdbc的索引从1开始
          ps.setLong(2,confirmedIncr)
          ps.setLong(3,confirmedCount)
          ps.setLong(4,suspectedCount)
          ps.setLong(5,curedCount)
          ps.setLong(6,deadCount)
          ps.executeUpdate()
        }
      })
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    // result4 输出到控制台
    result4.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()
    //.awaitTermination()

    // result4输出到MySQL
    result4.writeStream
      .foreach(new BaseJdbcSink("REPLACE INTO `abroad_import` (`datetime`, `provinceShortName`, `pid`, `confirmedCount`) VALUES (?, ?, ?, ?);") {
        override def realProcess(sql: String, row: Row): Unit = {//注意row的索引从0开始
          val datetime: String = row.getAs[String]("datetime")
          val provinceShortName: String = row.getAs[String]("provinceShortName")
          val pid: Int = row.getAs[Int]("pid")
          val confirmedCount: Long = row.getAs[Long]("confirmedCount")
          ps = conn.prepareStatement(sql)
          ps.setString(1,datetime)//jdbc的索引从1开始
          ps.setString(2,provinceShortName)
          ps.setInt(3,pid)
          ps.setLong(4,confirmedCount)
          ps.executeUpdate()
        }
      })
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    // 将结果5输出到控制台
    result5.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    // 将结果5输出到MySQL数据库
    result5.writeStream
      // 在2.2.0版本只能使用foreach,效率较低
      // 在2.4版本有foreachBatch,效率较高
      .foreach(new BaseJdbcSink("REPLACE INTO `beijing_confirmed` (`datetime`, `locationId`, `provinceShortName`, `cityName`,`currentConfirmedCount`, `confirmedCount`, `suspectedCount`, `curedCount`, `deadCount`) VALUES (?, ?, ?, ?, ?, ?, ?, ?);") {
        override def realProcess(sql: String, row: Row): Unit = {//注意row的索引从0开始
          val datetime: String = row.getAs[String]("datetime")
          val locationId: Int = row.getAs[Int]("locationId")
          val provinceShortName: String = row.getAs[String]("provinceShortName")
          val cityName: String = row.getAs[String]("cityName")
          val currentConfirmedCount: Int = row.getAs[Int]("currentConfirmedCount")
          val confirmedCount: Int = row.getAs[Int]("confirmedCount")
          val suspectedCount: Int = row.getAs[Int]("suspectedCount")
          val curedCount: Int = row.getAs[Int]("curedCount")
          val deadCount: Int = row.getAs[Int]("deadCount")

          ps = conn.prepareStatement(sql)
          ps.setString(1,datetime)//jdbc的索引从1开始
          ps.setInt(2,locationId)
          ps.setString(3,provinceShortName)
          ps.setString(4,cityName)
          ps.setInt(5,currentConfirmedCount)
          ps.setInt(6,confirmedCount)
          ps.setInt(7,suspectedCount)
          ps.setInt(8,curedCount)
          ps.setInt(9,deadCount)
          ps.executeUpdate()
        }
      })
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()
      .awaitTermination()
  }
}
