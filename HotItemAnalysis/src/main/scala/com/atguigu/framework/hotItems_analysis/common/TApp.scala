package com.atguigu.framework.hotItems_analysis.common

import com.atguigu.framework.hotItems_analysis.util.EnvUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

trait TApp {
  def start(jobName: String = "app")(op: => Unit): Any = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    EnvUtil.put(env)
    try {
      op
    } catch {
      case exception: Exception => print(exception.getMessage)
    }
    env.execute(jobName)
  }
}
