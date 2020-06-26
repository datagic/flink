package com.datagic.flink.datastream.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * Desc: 自定义的非并行Source
 * Author 云瞻
 */
class CustomNonParallelSourceFunction extends SourceFunction[Long] {

  var count = 1L

  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {

    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = {
    var isRunning = false
  }
}
