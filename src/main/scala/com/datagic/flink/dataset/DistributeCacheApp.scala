package com.datagic.flink.dataset

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * Desc: 分布式缓存
 * Author 云瞻
 * CreateDate 2020/6/26 2:08 上午
 */
object DistributeCacheApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = "/Users/datagic/Downloads/temp/input/hello.txt"

    // 注册一个本地/hdfs文件
    env.registerCachedFile(filePath, "datagic-scala-dc")

    val data = env.fromElements("hadoop", "spark", "flink", "hbase", "hive")

    data.map(new RichMapFunction[String, String] {
      override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache().getFile("datagic-scala-dc")
        val lines = FileUtils.readLines(dcFile)

        // java集合和scala集合不兼容 不能用 <- 但是可以转换之后再使用  <-
        // import scala.collection.JavaConversions._
        import scala.collection.JavaConverters._
        for (ele <- lines.asScala) {
          println(ele)
        }
      }

      override def map(value: String): String = {
        value
      }
    }).print()

  }
}