package com.atguigu.adClick_analysis

import com.atguigu.adClick_analysis.AdClick.blackSideOutPutTag
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.net.URL

case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class CountByProvince(windowEnd: Long, province: String, cnt: Long)

case class BlackWarning(userId: Long, adId: Long, msg: String)

object AdClick {

  val blackSideOutPutTag = new OutputTag[BlackWarning]("blackWarning")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource: URL = getClass.getResource("/AdClickLog.csv")
    val adLog: DataStream[AdClickLog] = env.readTextFile(resource.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val filteredBlackUserData = adLog
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100))

    filteredBlackUserData.keyBy(_.province)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new AdCountAgg(), new AdWindowFunction())
      .print()

    filteredBlackUserData.getSideOutput(blackSideOutPutTag).print("black list")

    env.execute("ad click")
  }
}

class FilterBlackListUser(maxCnt: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
  //保存当前用户对当前广告的点击量
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))

  //保存当前用户对当前广告的点击量是否超过阈值，也就是是否进入黑名单
  lazy val isBlack: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black-state", classOf[Boolean]))

  //保存定时器触发的时间戳（0点）,清空点击量状态
  lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))

  override def processElement(i: AdClickLog, context: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, collector: Collector[AdClickLog]): Unit = {
    //针对当前key，注册一个定时器，每天0点触发，清空计数状态
    val curCnt: Long = countState.value()
    if (curCnt == 0) {
      //说明是当前key的第一条数据
      val resetTS: Long = (context.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000)
      resetTime.update(resetTS)
      context.timerService().registerProcessingTimeTimer(resetTS)
    }
    if (curCnt >= maxCnt) {
      //说明之前没有加入过黑名单
      if (!isBlack.value()) {
        isBlack.update(true)
        context.output(blackSideOutPutTag, BlackWarning(i.userId, i.adId, "Click ad over " + maxCnt + " times today"))
      }
      //加入过了就之间return,不用多次输出警告
      return
    }
    countState.update(curCnt + 1)
    collector.collect(i)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if(timestamp == resetTime.value()){
      isBlack.clear()
      countState.clear()
    }
  }
}

class AdCountAgg extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdWindowFunction() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(window.getEnd, key, input.head))
  }
}

