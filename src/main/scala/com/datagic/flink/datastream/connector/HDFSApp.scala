package com.datagic.flink.datastream.connector

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink

/**
 * Desc: HDFS Connector
 * Author 云瞻
 */
object HDFSApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("192.168.188.200", 9999)

    val hdfsPath = "/Users/datagic/Downloads/temp/output/"

    val sink = new BucketingSink[String](hdfsPath)

    data.addSink(sink)

    env.execute()
  }

}
