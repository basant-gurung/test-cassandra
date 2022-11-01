package com.pkware.cassandra

import java.util.Base64
import java.util.Base64.Decoder
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

object DgSecurity {
  /*def main(args: Array[String]): Unit = {
    val a = encryptProperty("dataguise")
    println(a)
    println(URLEncoder.encode(decryptProperty(a), StandardCharsets.UTF_8.toString))
  }*/

  def encryptProperty(text: String): String = {
    val key: Array[Byte] = Array(0x1.toByte, 0x2.toByte, 0x1.toByte, 0x3.toByte, 0x2.toByte, 0x1.toByte, 0x1.toByte, 0x3.toByte, 0x1.toByte, 0x1.toByte, 0x5.toByte, 0x1.toByte, 0x3.toByte, 0x1.toByte, 0x4.toByte, 0x2.toByte)
    val iv: Array[Byte] = Array(0x3.toByte, 0x1.toByte, 0x5.toByte, 0x2.toByte, 0x4.toByte, 0x1.toByte, 0x1.toByte, 0x1.toByte, 0x3.toByte, 0x1.toByte, 0x1.toByte, 0x2.toByte, 0x5.toByte, 0x1.toByte, 0x2.toByte, 0x1.toByte)

    if (text != null && text.nonEmpty && !text.equalsIgnoreCase("null") && text != "-1") {
      val cipher: Cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
      val keySpec: SecretKeySpec = new SecretKeySpec(key, "AES")
      val ivSpec: IvParameterSpec = new IvParameterSpec(iv)
      cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec)
      try {
        val results: Array[Byte] = cipher.doFinal(text.getBytes("UTF-8"))
        val encoder: Base64.Encoder = java.util.Base64.getEncoder
        encoder.encodeToString(results)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      }
    }
    else text
  }

  def decryptProperty(text: String): String = {
    val key: Array[Byte] = Array(0x1.toByte, 0x2.toByte, 0x1.toByte, 0x3.toByte, 0x2.toByte, 0x1.toByte, 0x1.toByte, 0x3.toByte, 0x1.toByte, 0x1.toByte, 0x5.toByte, 0x1.toByte, 0x3.toByte, 0x1.toByte, 0x4.toByte, 0x2.toByte)
    val iv: Array[Byte] = Array(0x3.toByte, 0x1.toByte, 0x5.toByte, 0x2.toByte, 0x4.toByte, 0x1.toByte, 0x1.toByte, 0x1.toByte, 0x3.toByte, 0x1.toByte, 0x1.toByte, 0x2.toByte, 0x5.toByte, 0x1.toByte, 0x2.toByte, 0x1.toByte)

    if (text != null && text.nonEmpty && !text.equalsIgnoreCase("null") && text != "-1") {
      val cipher: Cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
      val keySpec: SecretKeySpec = new SecretKeySpec(key, "AES")
      val ivSpec: IvParameterSpec = new IvParameterSpec(iv)
      cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec)
      val decoder: Decoder = java.util.Base64.getMimeDecoder
      try {
        val results: Array[Byte] = cipher.doFinal(decoder.decode(text))
        new String(results, "UTF-8")
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      }
    }
    else text
  }
}
