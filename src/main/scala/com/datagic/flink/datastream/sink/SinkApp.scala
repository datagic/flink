package com.datagic.flink.datastream.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * Desc: DataStream Sink
 * Author 云瞻
 */
object SinkApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("localhost", 9999)

    val student = data.map(x => {
      if (x.length > 0) {
        x
      } else {
        ""
      }
    })

    student.addSink(new CustomSinkFunction).setParallelism(1)

    env.execute("SinkApp")

  }

}
