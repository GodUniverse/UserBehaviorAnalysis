package com.atguigu.framework.hotItems_analysis.controller

import com.atguigu.framework.hotItems_analysis.common.TController
import com.atguigu.framework.hotItems_analysis.service.HotItemsService

class HotItemsController extends TController {
  private val hotItemsService = new HotItemsService

  def execute() = {
    hotItemsService.analysis()
  }
}
