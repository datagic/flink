package com.datagic.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * Desc: DataSet Sink
 * Author 云瞻
 */
object SinkApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = 1 to 3

    val text = env.fromCollection(data)

    text.writeAsText("/Users/datagic/Downloads/temp/output/", WriteMode.OVERWRITE).setParallelism(5)

    env.execute("SinkApp")
  }

}
