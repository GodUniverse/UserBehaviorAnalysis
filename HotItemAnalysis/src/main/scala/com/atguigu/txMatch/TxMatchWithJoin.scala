package com.atguigu.txMatch

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.net.URL

object TxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取order信息
    val resource1: URL = getClass.getResource("/OrderLog.csv")
    val orderStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource1.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)
    //读取receipt信息
    val resource2: URL = getClass.getResource("/ReceiptLog.csv")
    val receiptStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(resource2.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderStream.intervalJoin(receiptStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new MyTxMatchProcessJoinFunction())

    resultStream.print("tx match interval join")

    env.execute("tx match with join")
  }
}

class MyTxMatchProcessJoinFunction extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  override def processElement(in1: OrderEvent, in2: ReceiptEvent, context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    collector.collect((in1, in2))
  }
}
