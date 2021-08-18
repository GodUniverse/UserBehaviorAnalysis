package com.atguigu.application

import com.atguigu.common.TApp
import com.atguigu.controller.{HotItemsController, HotItemsWithSqlController}

object HotItemsWithSQLApplication extends App with TApp {
  start("Hot Item Anaylsis With SQL") {
    val controller = new HotItemsWithSqlController()
    controller.execute()
  }
}
