package com.atguigu.framework.hotItems_analysis.service

import java.net.URL

import com.atguigu.framework.hotItems_analysis.bean.UserBehavior
import com.atguigu.framework.hotItems_analysis.dao.HotItemsDao
import com.atguigu.framework.hotItems_analysis.util.EnvUtil
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

class HotItemsWithSqlService {
  private val hotItemsDao = new HotItemsDao

  def analysis():Unit = {
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val datas: DataStream[String] = hotItemsDao.readFile(resource.getPath)
    val dataStream: DataStream[UserBehavior] = datas.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(EnvUtil.take(), settings)

    val datatable: Table = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    //Table API实现聚合
    val aggTable: Table = datatable.filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)

    //SQL实现topN
    tableEnv.createTemporaryView("aggTable", aggTable, 'itemId, 'windowEnd, 'cnt)
    val resultTable = tableEnv.sqlQuery(
      """
        |select * from
        | (select *,
        | row_number() over(partition by windowEnd order by cnt desc) as rk
        | from aggTable) t1
        |where rk <= 5
      """.stripMargin)

    resultTable.toRetractStream[Row].print()
  }
}
