package com.datagic.flink.datastream

import java.{lang, util}

import com.datagic.flink.datastream.source.CustomNonParallelSourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector

/**
 * Desc: DataStream Transformation
 * Author 云瞻
 */
object TransformationApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val data = env.addSource(new CustomNonParallelSourceFunction)

    // filter
    // data.map(x => {
    //   println("received: " + x)
    //   x
    // }).filter(_ % 2 == 0).print().setParallelism(1)

    // union
    // data.union(data).print().setParallelism(1)

    // split select
    val splits = data.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if (value % 2 == 0) {
          list.add("even")
        } else {
          list.add("odd")
        }
        list
      }
    })

    splits.select("odd").print().setParallelism(1)

    env.execute("TransformationApp")

  }

}
