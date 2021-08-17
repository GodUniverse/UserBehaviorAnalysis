package com.atguigu.controller

import com.atguigu.common.TController
import com.atguigu.service.HotItemsService

class HotItemsController extends TController {
  private val hotItemsService = new HotItemsService

  def execute() = {
    hotItemsService.analysis()
  }
}
