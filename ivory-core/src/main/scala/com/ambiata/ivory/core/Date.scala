package com.ambiata.ivory.core

import org.joda.time.LocalDate
import com.ambiata.mundane.parse.ListParser

case class Date(year: Short, month: Byte, day: Byte) {

  def int: Int =
    (year.toInt << 16) | (month.toInt << 8) | day.toInt

  def localDate: LocalDate =
    new LocalDate(year.toInt, month.toInt, day.toInt)

  def string(delim: String = "-"): String =
    s"%4d${delim}%02d${delim}%02d".format(year, month, day)

  def isBefore(other: Date): Boolean =
    int < other.int

  def isBeforeOrEqual(other: Date): Boolean =
    int <= other.int
}

object Date {

  def fromInt(i: Int): Date =
    Date(((i >>> 16) & 0xffff).toShort, ((i >>> 8) & 0xff).toByte, (i & 0xff).toByte)

  def fromLocalDate(d: LocalDate): Date =
    Date(d.getYear.toShort, d.getMonthOfYear.toByte, d.getDayOfMonth.toByte)

  def listParser: ListParser[Date] = {
    import ListParser._
    for {
      y        <- short
      m        <- short
      d        <- short
      position <- getPosition
      result   <- value(Date(y, m.toByte, d.toByte),
                        { t => s"""Not a part of a date at position $position: $t""" })
    } yield result
  }
}
