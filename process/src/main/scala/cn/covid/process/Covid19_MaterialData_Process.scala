package cn.covid.process

import cn.covid.util.OffsetUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.collection.mutable

/**
 * Author：
 * Date：2022/6/621:02
 * Desc: 疫情物资数据实时处理分析
 */
object Covid19_MaterialData_Process {
  def main(args: Array[String]): Unit = {
    // 1. 准备SparkStreaming的开发环境
    val conf: SparkConf = new SparkConf().setAppName("Covid19_MaterialData_Process").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")  // 设置日志级别
    val ssc:StreamingContext = new StreamingContext(sc, Seconds(5)) //第二个参数为批处理时间
    ssc.checkpoint(directory = "./sscckp")

    // 2. 准备Kafka的连接参数
    val kafkaParams:Map[String,Object] = Map[String,Object](
      "bootstrap.servers"->"192.168.29.129:9091,192.168.29.129:9092,192.168.29.129:9093",
      "group.id"->"SparkKafka",
      // 偏移量重置位置
      // latest表示如果记录了偏移量则从记录的位置开始消费，如果没有记录偏移量则从最新/最后的位置开始消费
      // earliest表示如果记录了偏移量则从记录的位置开始消费，如果没有记录偏移量则从最开始/最早的位置开始消费
      // none表示如果记录了偏移量则从记录的位置开始消费，如果没有记录偏移量则报错
      "auto.offset.reset"->"latest",
      "enable.auto.commit"->(false:java.lang.Boolean), //是否自动提交偏移量
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer]
    )

    val topics: Array[String] = Array("covid19_material")

    // 从Mysql中查询出offsets：Map[TopicPartition,Long]
    val offsetsMap:mutable.Map[TopicPartition,Long] = OffsetUtils.getOffsetMap("SparkKafka","covid19_material")
    val kafkaDs:InputDStream[ConsumerRecord[String,String]] = if(offsetsMap.size > 0) {
      println("MySQL记录了Offset信息，从Offset处开始消费")
      // 3. 连接kafka获取消息
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,offsetsMap)
      )
    }else{
      println("MySQL未记录Offset信息，从Latest处开始消费")
      // 3. 连接kafka获取消息
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    }

    // 4. 实时处理数据
    //    val valueDS: DStream[String] = kafkaDs.map(_.value())   //  _表示从kafka中消费出来的每一条消息
    // {"count":3313,"from":"采购","name":"一次性橡胶手套/副"}
    // 我们从Kafka中消费的数据为上树jsonStr，需要解析为json对象
    // 将接收到的数据转换为需要的元组格式：(name,(采购，下拨，捐赠，需求，库存))
    val tupleDS: DStream[(String,(Int,Int,Int,Int,Int,Int))] = kafkaDs.map(record => {
      val jsonStr: String = record.value()
      val jsonObject: JSONObject = JSON.parseObject(jsonStr)
      val name: String = jsonObject.getString("name")
      val from: String = jsonObject.getString("from")
      val count: Int = jsonObject.getInteger("count")
      // 根据物资来源不同将count记录在不同位置，最终形成统一格式
      from match{
        case "采购" => (name,(count,0,0,0,0,count))
        case "下拨" => (name,(0,count,0,0,0,count))
        case "捐赠" => (name,(0,0,count,0,0,count))
        case "消耗" => (name,(0,0,0,-count,0,-count))
        case "需求" => (name,(0,0,0,0,-count,-count))
      }
    })
    // 将上述数据按照key进行聚合(有状态的计算)--使用updateStateByKey
    // 定义一个函数用于将当前批次数据和历史数据进行聚合
    val updateFunc = (currentValues:Seq[(Int,Int,Int,Int,Int,Int)],historyValue:Option[(Int,Int,Int,Int,Int,Int)])=>{
      // 0. 定义变量用于接收当前批次数据(采购，下拨，捐赠，消耗，需求，库存)
      var current_buy: Int = 0
      var current_assign: Int = 0
      var current_donate: Int = 0
      var current_consume: Int = 0
      var current_needs: Int = 0
      var current_storage: Int = 0
      if(currentValues.size > 0){
        // 1.取出当前批次数据
        for(currentValue <- currentValues){
          current_buy += currentValue._1
          current_assign += currentValue._2
          current_donate += currentValue._3
          current_consume += currentValue._4
          current_needs += currentValue._5
          current_storage += currentValue._6
        }
        // 2.取出历史数据
        val history_buy: Int = historyValue.getOrElse((0,0,0,0,0,0))._1
        val history_assign: Int = historyValue.getOrElse((0,0,0,0,0,0))._2
        val history_donate: Int = historyValue.getOrElse((0,0,0,0,0,0))._3
        val history_consume: Int = historyValue.getOrElse((0,0,0,0,0,0))._4
        val history_needs: Int = historyValue.getOrElse((0,0,0,0,0,0))._5
        val history_storage: Int = historyValue.getOrElse((0,0,0,0,0,0))._6
        // 3.将当前批次数据与历史数据进行聚合
        val result_buy: Int = current_buy + history_buy
        val result_assign: Int = current_assign + history_assign
        val result_donate: Int = current_donate + history_donate
        val result_consume: Int = current_consume + history_consume
        val result_needs: Int = current_needs + history_needs
        val result_storage: Int = current_storage + history_storage

        // 4.将聚合结果返回
        Some((result_buy,result_assign,result_donate,result_consume,result_needs,result_storage))
      }else{
        historyValue
      }
    }
    val resultDS:DStream[(String,(Int,Int,Int,Int,Int,Int))] = tupleDS.updateStateByKey(updateFunc)
    resultDS.print()
    // 5.将处理分析的结果存入到MySql
    resultDS.foreachRDD(rdd =>{
      rdd.foreachPartition(lines => {
        // 1.开启连接
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/covid?characterEncoding=UTF-8","root", "123456")
        // 2.编写sql并获取连接
        val sql:String = "replace into covid_material(`name`,`buy`,`assign`,`donate`,`consume`,`needs`,`storage`) values(?,?,?,?,?,?,?)"
        val ps: PreparedStatement = conn.prepareStatement(sql)
        // 3.设置并执行参数
        for(line <- lines){
          ps.setString(1,line._1)
          ps.setInt(2,line._2._1)
          ps.setInt(3,line._2._2)
          ps.setInt(4,line._2._3)
          ps.setInt(5,line._2._4)
          ps.setInt(6,line._2._5)
          ps.setInt(7,line._2._6)
          ps.executeUpdate()
        }
        // 4.关闭资源
        ps.close()
        conn.close()
      })
    })


    // 6. 手动提交偏移量
      kafkaDs.foreachRDD(rdd=>{
        if(rdd.count() > 0){  //如果rdd中有数据则进行处理
          //rdd.foreach(record => println("从kafka中消费到的每一条消息：" + record))
          //从kafka中消费到的每一条消息：ConsumerRecord(topic = covid19_material, partition = 0, leaderEpoch = 10, offset = 29, CreateTime = 1654518171097, serialized key size = -1, serialized value size = 7, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = spark)
          // 获取偏移量，使用Sprak-streaming-kafka-0-10中封装好的API来存放偏移量并提交
          val offsets: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //for(o <- offsets) {
            //println(s"topics = ${o.topic},partition = ${o.partition},formOffset = ${o.fromOffset},util = ${o.untilOffset}")
            //topics = covid19_material,partition = 0,util=30
          //}
          // 手动提交偏移量到kafka的默认主题：__consumer__offsets中，如果开启了Checkpoint还会提交到Checkpoint中
          // kafkaDs.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
          OffsetUtils.saveOffsets("SparkKafka",offsets)
        }
      })

    // 7.开启SparkStreaming任务并等待结束
    ssc.start()
    ssc.awaitTermination()
  }

}
