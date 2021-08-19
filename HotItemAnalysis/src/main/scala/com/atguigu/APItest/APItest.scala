package com.atguigu.APItest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

case class User(id:Long,name:String,age:Int)

object APItest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val data: DataStream[String] = env.readTextFile("F:\\study\\IdeaProjects\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\test.txt")
    data.map(line => {
      val arr: Array[String] = line.split(",")
      User(arr(0).toLong,arr(1),arr(2).toInt)
    })
      .keyBy(_.age)
      .reduce((x,y) => User(x.id.max(y.id),x.name,x.age))
      .print("xxx")

    env.execute("api test")
  }
}
