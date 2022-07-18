package cn.covid.util

import org.apache.spark.sql.{ForeachWriter, Row}

import java.sql.{Connection,DriverManager, PreparedStatement}

/**
 * Author：
 * Date：2022/6/1321:25
 * Desc:
 */
abstract class BaseJdbcSink(sql:String) extends ForeachWriter[Row]{
  var conn: Connection = _

  var ps: PreparedStatement = _

  // 开启连接
  override def open(partitionId: Long, version: Long): Boolean = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/covid19?characterEncoding=UTF-8","root","123456")
    true
  }

  // 处理数据-将数据存入到MySQL数据库
  override def process(value: Row): Unit = {
    realProcess(sql,value)
  }

  def realProcess(sql:String,value: Row)

  // 关闭连接
  override def close(errorOrNull: Throwable): Unit = {
    if (conn != null) {
      conn.close
    }
    if (ps != null) {
      ps.close()
    }
  }
}
