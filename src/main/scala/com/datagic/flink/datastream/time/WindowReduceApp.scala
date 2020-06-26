package com.datagic.flink.datastream.time

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Desc: window reduce 增量聚合
 * Author 云瞻
 */
object WindowReduceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)
    text.flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)
      .map(x => (1, x.toInt)) // 1,2,3,4,5   (1,1),(1,2),(1,3),(1,4),(1,5)
      .keyBy(0) // key 相同 所以所有元素到一个task执行
      //窗口大小 滑动大小，每隔5秒 统计近10秒数据
      .timeWindow(Time.seconds(5))
      .reduce((v1, v2) => {
        (v1._1, v2._1 + v2._2)
      }) //窗口的增量操作
      .print()
      .setParallelism(1)

    env.execute()
  }

}
