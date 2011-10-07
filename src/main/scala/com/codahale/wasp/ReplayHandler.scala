package com.codahale.wasp

trait ReplayHandler[A, B] {
  def process(id: Long, data: A)

  def result(): B
}
