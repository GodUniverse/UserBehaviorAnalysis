package com.atguigu.txMatch

import com.atguigu.txMatch.TxMatch.{unmatchedPays, unmatchedReceipts}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.net.URL

case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMatch {

  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

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

    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderStream
      .connect(receiptStream)
      .process(new MyCoProcessFunction())

    resultStream.print("matched")
    resultStream.getSideOutput(unmatchedPays).print("unmatched-pay")
    resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipt")

    env.execute("tx match job")
  }
}

class MyCoProcessFunction extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

  lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

  override def processElement1(in1: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receiptEvent: ReceiptEvent = receiptState.value()
    if (receiptEvent != null) {
      collector.collect((in1, receiptEvent))
      receiptState.clear()
    } else {
      payState.update(in1)
      context.timerService().registerEventTimeTimer(in1.eventTime * 1000L + 5000L)
    }
  }

  override def processElement2(in2: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val orderEvent: OrderEvent = payState.value()
    if (orderEvent != null) {
      collector.collect((orderEvent, in2))
      payState.clear()
    } else {
      receiptState.update(in2)
      context.timerService().registerEventTimeTimer(in2.eventTime * 1000L + 3000L)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //说明pay来了，receipt超时没来
    if (payState.value() != null) {
      //使用pay的测输出流
      ctx.output(unmatchedPays, payState.value())
    }
    if (receiptState.value() != null) {
      ctx.output(unmatchedReceipts, receiptState.value())
    }
    payState.clear()
    receiptState.clear()
  }
}