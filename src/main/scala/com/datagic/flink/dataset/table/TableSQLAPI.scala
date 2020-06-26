package com.datagic.flink.dataset.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * Desc:  DataSet Table SQL API
 * Author 云瞻
 */
object TableSQLAPI {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val filepath = "/Users/datagic/Downloads/temp/input/student.csv"
    val studentCsv = env.readCsvFile[Student](filepath, ignoreFirstLine = true)

    // dataset 转 table
    val studentTable = tableEnv.fromDataSet(studentCsv)

    // table 注册成 表
    tableEnv.registerTable("studentTable", studentTable)

    val res = tableEnv.sqlQuery(
      """
        |select * from studentTable where id in (1,3)
        |""".stripMargin)

    tableEnv.toDataSet[Row](res).print()

  }

  case class Student(id: Int, name: String, age: Int)

}
