package com.atguigu.dao

import java.util.Properties

import com.atguigu.util.EnvUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._

class HotItemsDao {
  def readFile(path: String) = {
    EnvUtil.take.readTextFile(path)
  }

  def readKafka(prop: Properties) = {
    EnvUtil.take.addSource(new FlinkKafkaConsumer[String]("hotitems",
      new SimpleStringSchema(),
      prop))
  }
}
