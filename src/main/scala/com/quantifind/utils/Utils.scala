package com.quantifind.utils

import scala.util.{Failure, Success, Try}

/**
 * Basic utils, e.g. retry block
 *
 * @author xorlev
 */

object Utils {
  val regex = """((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)""".r
  val regexStr = """^EndPoint""".r
  // Returning T, throwing the exception on failure
  @annotation.tailrec
  final def retry[T](n: Int)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case Failure(e) => throw e
    }
  }

  def ipMatch(ip:String): String ={
//    val result = regexStr.findFirstIn(ip)
//    println(result)
//    result.getOrElse("localhost")
    val result = (ip.split("EndPoint")(1)).split("ListenerName")(0).replace("(","")
    result
  }

  def getClusterList(broker:String): String ={
    val hostAndPort = ipMatch(broker)
    val brokerId = broker.split(":")(0).trim
    val host = hostAndPort.split(",")(0).trim
    val port = hostAndPort.split(",")(1).trim
    brokerId+" : "+host+" : "+port
  }

  def main(args: Array[String]): Unit = {
    val result = ipMatch("(EndPoint(10.9.163.197,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"10 : (EndPoint(10.9.186.80,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"11 : (EndPoint(10.9.103.252,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"12 : (EndPoint(10.9.161.48,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"13 : (EndPoint(10.9.102.29,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"14 : (EndPoint(10.9.191.175,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"15 : (EndPoint(10.9.167.232,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"16 : (EndPoint(10.9.136.93,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"17 : (EndPoint(10.9.83.152,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"18 : (EndPoint(10.9.124.28,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"19 : (EndPoint(10.9.134.113,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"2 : (EndPoint(10.9.180.228,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"20 : (EndPoint(10.9.107.19,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"3 : (EndPoint(10.9.127.251,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"4 : (EndPoint(10.9.110.43,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"5 : (EndPoint(10.9.152.48,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"6 : (EndPoint(10.9.139.58,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"7 : (EndPoint(10.9.81.62,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"8 : (EndPoint(10.9.104.63,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]},{\"name\":\"9 : (EndPoint(10.9.185.70,9092,ListenerName(PLAINTEXT),PLAINTEXT)) : null\",\"children\":[]}]}")
    println(result.toString)
  }
}
