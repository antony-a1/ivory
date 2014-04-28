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

  /**
   * This is not epoch! Add seconds to the int representation of the date. This will
   * produce a long that can be reversed with the Date.fromSeconds function.
   */
  def addSeconds(s: Int): Long =
    int.toLong << 32 | s
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

  /**
   * This is not epoch! It will take a long which was created from Date.addSeconds and
   * pull the original Date and seconds out.
   */
  def fromSeconds(s: Long): (Date, Int) =
    (fromInt(((s >>> 32) & 0xffffffff).toInt), (s & 0xffff).toInt)
}
