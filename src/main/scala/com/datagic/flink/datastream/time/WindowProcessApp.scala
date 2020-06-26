package com.datagic.flink.datastream.time

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction

/**
 * Desc: window process 全量聚合 先存buffer中
 * Author 云瞻
 */
object WindowProcessApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)
    text.flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)
      .map(x => (1, x.toInt)) // 1,2,3,4,5   (1,1),(1,2),(1,3),(1,4),(1,5)
      .keyBy(_._1) // key 相同 所以所有元素到一个task执行
      //窗口大小 滑动大小，每隔5秒 统计近10秒数据
      .timeWindow(Time.seconds(5))
      .process(new ProcessWindowFunction[(Int, Int), (Int, Int), Int, TimeWindow] {
        override def process(key: Int, context: Context, in: Iterable[(Int, Int)], out: Collector[(Int, Int)]): Unit = {
          var count = 0
          for (i <- in) {
            count = count + i._2
          }
          println(context.currentProcessingTime)
          out.collect(1, count)
        }
      }).print().setParallelism(1)

    env.execute()
  }

}
