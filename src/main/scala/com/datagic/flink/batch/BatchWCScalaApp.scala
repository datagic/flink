package com.datagic.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * Desc: 使用Scala开发Flink的批处理应用程序
 * Author 云瞻
 * CreateDate 2020/6/25 9:16 下午
 */
object BatchWCScalaApp {

  def main(args: Array[String]): Unit = {
    val input = "/Users/datagic/Downloads/temp/input"

    // 1.获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2. 获取数据
    val text = env.readTextFile(input)

    //隐式转换
    import org.apache.flink.api.scala._

    // 3.业务逻辑
    text.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }

}