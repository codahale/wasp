package com.codahale.wasp.tests

import com.codahale.wasp.Codec
import java.nio.charset.Charset
import java.lang.String

object StringCodec extends Codec[String] {
  private val utf8 = Charset.forName("UTF-8")

  def recordType = 0

  def encode(a: String) = a.getBytes(utf8)

  def decode(bytes: Array[Byte]) = new String(bytes, utf8)
}
