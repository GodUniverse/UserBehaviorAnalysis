package com.atguigu.dao

import com.atguigu.util.EnvUtil


class HotItemsDao {
  def readFile(path: String) = {
    EnvUtil.take.readTextFile(path)
  }
}
