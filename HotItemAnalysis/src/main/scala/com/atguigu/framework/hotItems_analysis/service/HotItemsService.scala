package com.atguigu.framework.hotItems_analysis.service

import java.sql.Timestamp
import java.util
import java.util.Properties

import com.atguigu.framework.hotItems_analysis.bean.{ItemViewCount, UserBehavior}
import com.atguigu.framework.hotItems_analysis.common.TService
import com.atguigu.framework.hotItems_analysis.dao.HotItemsDao
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


class HotItemsService extends TService {
  private val hotItemsDao = new HotItemsDao

  def analysis() = {
    //    val path = "F:\\study\\IdeaProjects\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv"
    //    val datas: DataStream[String] = hotItemsDao.readFile(path)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "hot1")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val datas: DataStream[String] = hotItemsDao.readKafka(properties)
    val result: DataStream[ItemViewCount] = datas
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())

    result.keyBy("windowEnd")
      .process(new TopNHotItems(5))
      .print()
  }
}

class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, window.getEnd, count))
  }
}

class TopNHotItems(topN: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  private var itemListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemListState", classOf[ItemViewCount]))
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemListState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    val iter: util.Iterator[ItemViewCount] = itemListState.get().iterator()
    while (iter.hasNext) {
      allItems += iter.next()
    }
    itemListState.clear()
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topN)

    val result = new StringBuilder
    result.append("====================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedItems.indices) {
      val currentItem: ItemViewCount = sortedItems(i)
      result.append("No").append(i + 1).append(":")
        .append(" 商品 ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")

    Thread.sleep(2000)
    out.collect(result.toString())
  }
}

