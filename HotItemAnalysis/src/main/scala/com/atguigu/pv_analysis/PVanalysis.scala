package com.atguigu.pv_analysis

import java.net.URL

import com.atguigu.framework.hotItems_analysis.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class PvWinCount(windowEnd: Long, cnt: Long)

object PVanalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    env.readTextFile(resource.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000l)
      .filter(_.behavior == "pv")
      .map(x => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAggFunction(), new PvWindowResultFunction())
      .print()

    env.execute("pv")
  }
}

class PvCountAggFunction extends AggregateFunction[(String, Int), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Int), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PvWindowResultFunction extends WindowFunction[Long, PvWinCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvWinCount]): Unit = {
    out.collect(PvWinCount(window.getEnd, input.head))
  }
}
