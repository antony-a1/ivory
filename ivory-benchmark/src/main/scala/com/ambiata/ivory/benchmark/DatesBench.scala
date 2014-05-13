package com.ambiata.ivory.benchmark

import com.ambiata.ivory.core._
import com.google.caliper._
import org.joda.time._, format.DateTimeFormat
import scalaz._, Scalaz._

object DatesBenchApp extends App {
  Runner.main(classOf[DatesBench], args)
}

case class DatesBench() extends SimpleScalaBenchmark {
  def timejoda(n: Int) =
    repeat[String \/ Date](n) {
      try {
        val d = DateTimeFormat.forPattern("yyyy-MM-dd").parseLocalDate("2012-01-01")
        Date.unsafe(d.getYear.toShort, d.getMonthOfYear.toByte, d.getDayOfMonth.toByte).right[String]
      } catch { case e: Throwable => "bad".left }
    }

  def timejodabad(n: Int) =
    repeat[String \/ Date](n) {
      try {
        val d = DateTimeFormat.forPattern("yyyy-MM-dd").parseLocalDate("201x-01-01")
        Date.unsafe(d.getYear.toShort, d.getMonthOfYear.toByte, d.getDayOfMonth.toByte).right[String]
      } catch { case e: Throwable => "bad".left }
    }

  def timeregex(n: Int) = {
    val DateParser = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
    repeat[String \/ Date](n) {
       "2012-01-01" match {
         case DateParser(y, m, d) =>
           try Date.create(y.toShort, m.toByte, d.toByte).get.right
           catch { case e: Throwable => "bad".left }
         case _ => "bad".left
       }
    }
  }

  def timeregexbad(n: Int) = {
    val DateParser = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
    repeat[String \/ Date](n) {
       "201x-01-01" match {
         case DateParser(y, m, d) =>
           Date.create(y.toShort, m.toByte, d.toByte).map(_.right[String]).getOrElse("bad".left)
         case _ => "bad".left
       }
    }
  }

  def timedanger(n: Int) =
    repeat[String \/ Date](n) {
       val d = "2012-01-01"
       if (d.length != 10 || d.charAt(4) != '-' || d.charAt(7) != '-')
         "bad".left
       else try
         Date.create(d.substring(0, 4).toShort, d.substring(5, 7).toByte, d.substring(9, 10).toByte).get.right
       catch { case e: Throwable => "bad".left }
    }

  def timedegenerate(n: Int) =
    repeat[String \/ Date](n) {
       val d = "201x-01-01"
       if (d.length != 10 || d.charAt(4) != '-' || d.charAt(7) != '-')
         "bad".left
       else try
         Date.create(d.substring(0, 4).toShort, d.substring(5, 7).toByte, d.substring(9, 10).toByte).get.right
       catch { case e: Throwable => "bad".left }
    }

}
