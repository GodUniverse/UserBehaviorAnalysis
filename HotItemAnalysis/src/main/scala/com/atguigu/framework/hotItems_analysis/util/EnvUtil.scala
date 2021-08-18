package com.atguigu.framework.hotItems_analysis.util

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object EnvUtil {
  private val streamEnv = new ThreadLocal[StreamExecutionEnvironment]

  def put(env: StreamExecutionEnvironment): Unit = {
    streamEnv.set(env)
  }

  def take() = {
    streamEnv.get()
  }

  def clear() = {
    streamEnv.remove()
  }
}
