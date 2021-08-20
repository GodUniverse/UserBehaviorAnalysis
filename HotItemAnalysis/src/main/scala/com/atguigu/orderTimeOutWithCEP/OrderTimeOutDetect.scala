package com.atguigu.orderTimeOutWithCEP

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.net.URL
import java.util

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

case class OrderResult(orderId: Long, msg: String)

object OrderTimeOutDetect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource: URL = getClass.getResource("/OrderLog.csv")
    val data: KeyedStream[OrderEvent, Long] = env.readTextFile(resource.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    val orderPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    val timeoutSide: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeOut")

    val orderpatternStream: PatternStream[OrderEvent] = CEP.pattern[OrderEvent](data, orderPattern)

    val result: DataStream[OrderResult] = orderpatternStream.select(timeoutSide, new OrderTimeOutFunction(), new OrderSelectFunction())

    result.print("payed")
    result.getSideOutput(timeoutSide).print("timeout")

    env.execute("order timeout detect")
  }
}

class OrderTimeOutFunction extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeOutOrderId: Long = map.get("create").get(0).orderId
    OrderResult(timeOutOrderId,"timeout: " + l)
  }
}

class OrderSelectFunction extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId: Long = map.get("pay").get(0).orderId
    OrderResult(payedOrderId,"pay successfully")
  }
}
