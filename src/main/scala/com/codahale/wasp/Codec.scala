package com.codahale.wasp

import java.nio.charset.Charset

trait Codec[A] {
  def recordType: Byte
  def encode(a: A): Array[Byte]
  def decode(bytes: Array[Byte]): A
}

object ByteArrayCodec extends Codec[Array[Byte]] {
  def recordType = 0
  def encode(a: Array[Byte]) = a
  def decode(bytes: Array[Byte]) = bytes
}

object StringCodec extends Codec[String] {
  private val utf8 = Charset.forName("UTF-8")
  def recordType = 1
  def encode(a: String) = a.getBytes(utf8)
  def decode(bytes: Array[Byte]) = new String(bytes, utf8)
}
