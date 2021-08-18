package com.atguigu.framework.hotItems_analysis.controller

import com.atguigu.framework.hotItems_analysis.service.HotItemsWithSqlService


class HotItemsWithSqlController {
  private val hotItemsWithSqlService = new HotItemsWithSqlService

  def execute() = {
    hotItemsWithSqlService.analysis()
  }
}
