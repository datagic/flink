package com.datagic.flink.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Desc: Scala流处理 word count
 * Author 云瞻
 * CreateDate 2020/6/25 10:02 下午
 */
object StreamingWCScalaApp {
  def main(args: Array[String]): Unit = {
    // 1.获取流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 获取数据
    val text = env.socketTextStream("localhost", 9999)

    // 3.业务逻辑
    text.flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    // 4. 执行
    env.execute("StreamingWCScalaApp")
  }
}
