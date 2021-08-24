package com.atguigu.uv_analysis

import com.atguigu.framework.hotItems_analysis.bean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import java.lang
import java.net.URL

object UvWithBloom {
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
      .map(data => ("uv",data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())

    env.execute("uv with bloom")
  }
}

class MyTrigger extends Trigger[(String,Long),TimeWindow]{
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //每来一条数据就触发窗口操作，执行并清空窗口内数据
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = { }
}

class UvCountWithBloom extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{
  lazy val jedis = new Jedis("hadoop103", 6379)
  lazy val myBloom = new MyBloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //redis中bitmap的key
    val windowEnd= context.window.getEnd.toString
    val uv_count = "uv_count"
    // uv_count是redis hash表的key, windowEnd是field,window_uvcount是value值
    var window_uvcount = 0L
    if (jedis.hget(uv_count, windowEnd) != null){
      window_uvcount = jedis.hget(uv_count, windowEnd).toLong
    }

    val userId = elements.last._2.toString
    val offset: Long = myBloom.hash(userId, 26)

    val isExist: lang.Boolean = jedis.getbit(windowEnd, offset)
    if(!isExist){
      jedis.setbit(windowEnd,offset,true)
      jedis.hset(uv_count,windowEnd,(window_uvcount + 1).toString)
    }
  }
}

class MyBloom(size: Long) extends Serializable {
  private val cap = size

  //传入一个字符串，获得在bitmap中的位置
  def hash(value: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until value.length) {
      // 最简单的hash算法，每一位字符的ascii码值，乘以seed之后，做叠加
      result = result * seed + value.charAt(i)
    }
    (cap - 1) & result
  }
}