package com.atguigu.framework.hotItems_analysis.application

import com.atguigu.framework.hotItems_analysis.common.TApp
import com.atguigu.framework.hotItems_analysis.controller.{HotItemsController, HotItemsWithSqlController}

object HotItemsWithSQLApplication extends App with TApp {
  start("Hot Item Anaylsis With SQL") {
    val controller = new HotItemsWithSqlController()
    controller.execute()
  }
}
