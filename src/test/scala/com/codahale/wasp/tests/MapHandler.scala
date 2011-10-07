package com.codahale.wasp.tests

import scala.collection.SortedMap
import com.codahale.wasp.ReplayHandler

class MapHandler[A] extends ReplayHandler[A, SortedMap[Long, A]] {
  private val results = SortedMap.newBuilder[Long, A]

  def result() = results.result()

  def process(id: Long, data: A) {
    results += id -> data
  }
}
