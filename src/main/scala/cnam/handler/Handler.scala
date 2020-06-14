package cnam.handler

trait Handler {
  def run(interval: Int, historyDays: Int): Unit
}


