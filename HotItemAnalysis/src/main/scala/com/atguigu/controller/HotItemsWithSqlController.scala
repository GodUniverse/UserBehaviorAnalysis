package com.atguigu.controller

import com.atguigu.service.HotItemsWithSqlService


class HotItemsWithSqlController {
  private val hotItemsWithSqlService = new HotItemsWithSqlService

  def execute() = {
    hotItemsWithSqlService.analysis()
  }
}
