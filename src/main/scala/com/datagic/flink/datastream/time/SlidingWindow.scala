package com.datagic.flink.datastream.time

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Desc: 滑动窗口  每隔半小时，统计近一个小时的数据
 * Author 云瞻
 */
object SlidingWindow {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)

    text.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      //窗口大小 滑动大小，每隔5秒 统计近10秒数据
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute()
  }

}
