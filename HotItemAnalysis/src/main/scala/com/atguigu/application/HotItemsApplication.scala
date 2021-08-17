package com.atguigu.application

import com.atguigu.common.TApp
import com.atguigu.controller.HotItemsController

object HotItemsApplication extends App with TApp {
  start("Hot Item Anaylsis") {
    val hotItemsController = new HotItemsController()
    hotItemsController.execute()
  }
}
