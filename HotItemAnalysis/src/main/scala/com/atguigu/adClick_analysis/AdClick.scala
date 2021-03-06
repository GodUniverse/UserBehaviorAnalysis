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
  //?????????????????????????????????????????????
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))

  //????????????????????????????????????????????????????????????????????????????????????????????????
  lazy val isBlack: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black-state", classOf[Boolean]))

  //????????????????????????????????????0??????,?????????????????????
  lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))

  override def processElement(i: AdClickLog, context: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, collector: Collector[AdClickLog]): Unit = {
    //????????????key?????????????????????????????????0??????????????????????????????
    val curCnt: Long = countState.value()
    if (curCnt == 0) {
      //???????????????key??????????????????
      val resetTS: Long = (context.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000)
      resetTime.update(resetTS)
      context.timerService().registerProcessingTimeTimer(resetTS)
    }
    if (curCnt >= maxCnt) {
      //????????????????????????????????????
      if (!isBlack.value()) {
        isBlack.update(true)
        context.output(blackSideOutPutTag, BlackWarning(i.userId, i.adId, "Click ad over " + maxCnt + " times today"))
      }
      //?????????????????????return,????????????????????????
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

