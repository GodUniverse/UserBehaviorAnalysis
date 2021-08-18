package com.atguigu.hotPages_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object HotPages {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val datas: DataStream[ApacheLogEvent] = env.readTextFile("F:\\study\\IdeaProjects\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\apache.log")
      .map(line => {
        val arr: Array[String] = line.split(" ")
        val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = dateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(2), timestamp.toLong, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = {
          t.timestamp
        }
      })

    val aggStream: DataStream[UrlViewCount] = datas
      .filter(data => {
        val pattern = "^((?!\\.(css|js)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate(new PageCountAgg(), new WindowResutlFunction())

    aggStream.keyBy(_.windowEnd)
      .process(new TopNhotUrls(3))
      .print()

    env.execute("hot pages")
  }
}

class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowResutlFunction() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNhotUrls(top_number: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  lazy val urlMapState = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long])
  )

  override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    urlMapState.put(i.url, i.count)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    context.timerService().registerEventTimeTimer(i.windowEnd + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    if (timestamp == ctx.getCurrentKey + 60000l) {
      urlMapState.clear()
      return
    }

    val url_counts: ListBuffer[(String, Long)] = ListBuffer()
    val iter: util.Iterator[Map.Entry[String, Long]] = urlMapState.entries().iterator()
    while (iter.hasNext) {
      val entry: Map.Entry[String, Long] = iter.next()
      url_counts += ((entry.getKey, entry.getValue))
    }

    val sortedResult: ListBuffer[(String, Long)] = url_counts.sortBy(_._2)(Ordering.Long.reverse).take(top_number)

    var result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ")
      .append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedResult.indices) {
      result.append("No").append(i + 1).append(":")
        .append("URL=").append(sortedResult(i)._1.toString)
        .append("流量=").append(sortedResult(i)._2.toString).append("\n")
    }
    result.append("====================================\n\n")

    Thread.sleep(3000)
    out.collect(result.toString())
  }
}
