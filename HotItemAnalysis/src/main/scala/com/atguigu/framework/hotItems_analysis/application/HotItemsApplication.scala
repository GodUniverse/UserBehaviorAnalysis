package com.atguigu.framework.hotItems_analysis.application

import com.atguigu.framework.hotItems_analysis.common.TApp
import com.atguigu.framework.hotItems_analysis.controller.HotItemsController

object HotItemsApplication extends App with TApp {
  start("Hot Item Anaylsis") {
    val hotItemsController = new HotItemsController()
    hotItemsController.execute()
  }
}
