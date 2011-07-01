package com.codahale.wasp

trait Codec[A] {
  def recordType: Byte
  def encode(a: A): Array[Byte]
  def decode(bytes: Array[Byte]): A
}
