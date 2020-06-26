package com.datagic.flink.datastream.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * Desc: DataStream API
 * Author 云瞻
 */
object SourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    val data = env.addSource(new CustomNonParallelSourceFunction).setParallelism(1)
    //    val data = env.addSource(new CustomParallelSourceFunction).setParallelism(2)
    val data = env.addSource(new CustomRichParallelSourceFunction).setParallelism(2)

    data.print()

    env.execute("SourceAppJob")
  }

}
