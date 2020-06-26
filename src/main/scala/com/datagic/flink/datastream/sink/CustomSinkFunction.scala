package com.datagic.flink.datastream.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * Desc: 自定义SinkFunction
 * Author 云瞻
 */
class CustomSinkFunction extends RichSinkFunction[String] {

  // 初始化信息
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    println("获取数据库连接")
  }

  // 逻辑代码
  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    // 存入数据库
    if (value.length > 0) {
      println("存入数据库: " + value)
    } else {
      println("数据不正确，忽略")
    }
  }

  // 关闭
  override def close(): Unit = {
    super.close()
  }
}
