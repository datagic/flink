package com.datagic.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * Desc: DataSet 数据源获取方式
 * Author 云瞻
 */
object SourceApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从fromCollection
    // env.fromCollection(22 to 33).print()

    // 从文件夹下 或者 某文件
    // val text = env.readTextFile("/Users/datagic/Downloads/temp/input")
    // text.print()

    // csv文件
    // val csvFile = "/Users/datagic/Downloads/temp/input/people.csv"
    // 忽略首行
    // env.readCsvFile[(String, Int, String)](file, ignoreFirstLine = true).print()
    // 读两列
    // env.readCsvFile[(String, Int)](file, ignoreFirstLine = true, includedFields = Array(0, 1)).print()
    // env.readCsvFile[MyCaseClass](file, ignoreFirstLine = true, includedFields = Array(0, 1)).print()
    // case class MyCaseClass(name: String, age: Int)

    // 读取递归文件夹
    // val parameters = new Configuration
    // parameters.setBoolean("recursive.file.enumeration", true)
    // env.readTextFile("/Users/datagic/Downloads/temp/input").withParameters(parameters)

    //压缩文件 flink 的 readTextFile 可以自动进行解压

  }

}
