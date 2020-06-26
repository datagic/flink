package com.datagic.flink.dataset

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * Desc: 计数器
 * Author 云瞻
 */
object CountApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("hadoop", "spark", "flink", "HBase", "Hive")

    val info = data.map(new RichMapFunction[String, String] {
      // 定义计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // 注册计数器
        getRuntimeContext.addAccumulator("elem-counts-scala", counter)

      }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })

    info.writeAsText("/Users/datagic/Downloads/temp/output/", WriteMode.OVERWRITE).setParallelism(3)

    val jobResult = env.execute("CountApp")
    val num = jobResult.getAccumulatorResult[Long]("elem-counts-scala")

    println(num)

  }
}
