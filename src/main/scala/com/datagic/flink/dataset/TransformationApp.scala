package com.datagic.flink.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * Desc: DataSet Transformation
 * Author 云瞻
 */
object TransformationApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 2, 1)).setParallelism(2)

    // map算子  y=f(x)
    // data.map(_ + 1).print()

    // filter 过滤
    // data.filter(_ < 5).print()

    // mapPartition 每一个分区（并行度）调用一次
    // data.mapPartition(x => {
    //   println("获取数据库连接")
    //   println("关闭数据库连接")
    //   x
    // }).print()

    // first-n 返回前几条
    // data.first(2).print()
    // data.groupBy(0).first(2) // 每一组取前两条
    // data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2) // 分组 排序 取每组排序前2

    // flatMap   1 -> 多
    // val info = new ListBuffer[String]()
    // info.append("hadoop", "flink")
    // info.append("hadoop", "spark")
    // info.append("kafka", "flink")
    // val text = env.fromCollection(info)
    // text.flatMap(_.split(",")).print()
    // word count
    // text.flatMap(_.split(","))
    //   .map((_, 1))
    //   .groupBy(0)
    //   .sum(1)
    //   .print()

    // distinct
    // data.distinct().print()

    // join 内连接
    val info1 = new ListBuffer[(Int, String)]()
    info1.append((0, "datagic"))
    // info1.append((1, "datagic"))
    info1.append((1, "tangsan"))
    info1.append((2, "xiaowu"))
    val info2 = new ListBuffer[(Int, String)]()
    info2.append((1, "20"))
    info2.append((2, "30"))
    info2.append((3, "40"))

    val ds1 = env.fromCollection(info1)
    val ds2 = env.fromCollection(info2)

    // where 左边的key    equalTo右边的key
    // ds1.join(ds2).where(0).equalTo(0).apply((first, second) => {
    //   (first._1, first._2, second._2)
    // }).print()

    // ds1.leftOuterJoin(ds2).where(0).equalTo(0).apply((first, second) => {
    //   if (second == null) {
    //     (first._1, first._2, "-")
    //   } else {
    //     (first._1, first._2, second._2)
    //   }
    // }).print()

    // cross join
    // ds1.cross(ds2).print()

    // union
    // ds1.union(ds2).print()

  }

}
