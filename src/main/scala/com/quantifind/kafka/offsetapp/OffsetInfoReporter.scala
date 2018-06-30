package com.quantifind.kafka.offsetapp

import com.quantifind.kafka.OffsetGetter.OffsetInfo

trait OffsetInfoReporter {
  def report(info: IndexedSeq[OffsetInfo])
  def cleanupOldData() = {}
}
