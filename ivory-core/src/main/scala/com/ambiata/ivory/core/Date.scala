package com.ambiata.ivory.core

import scalaz._, Scalaz._
import org.joda.time.LocalDate
import com.ambiata.mundane.parse.ListParser

/* a packed int | 16 bits: year represented as a short | 8 bits: month represented as a byte | 8 bits: day represented as a byte | */
class Date private(val underlying: Int) extends AnyVal {
  def year: Short =
    (underlying >>> 16).toShort

  def month: Byte =
    (underlying >>> 8 & 0xff).toByte

  def day: Byte =
    (underlying & 0xff).toByte

  def int: Int =
    underlying

  def localDate: LocalDate =
    new LocalDate(year.toInt, month.toInt, day.toInt)

  def hyphenated: String =
    string("-")

  def string(delim: String): String =
    s"%4d${delim}%02d${delim}%02d".format(year, month, day)

  def isBefore(other: Date): Boolean =
    int < other.int

  def isBeforeOrEqual(other: Date): Boolean =
    int <= other.int

  def isAfter(other: Date): Boolean =
  int > other.int

  def isAfterOrEqual(other: Date): Boolean =
  int >= other.int

  def addTime(t: Time): DateTime =
    DateTime.unsafeFromLong(int.toLong << 32 | t.seconds)

  override def toString: String =
    s"Date($year,$month,$day)"
}


object Date {
  def apply(year: Short, month: Byte, day: Byte): Date =
    macro Macros.literal

  def unsafe(year: Short, month: Byte, day: Byte): Date =
    new Date((year.toInt << 16) | (month.toInt << 8) | day.toInt)

  def create(year: Short, month: Byte, day: Byte): Option[Date] = {
    def divisibleBy(n: Int, divisor: Int) = ((n / divisor) * divisor) == n
    def leapYear = divisibleBy(year, 4) && (!divisibleBy(year, 100) || divisibleBy(year, 400))
    (year >= 1000 && year <= 3000 && month >= 1 && month <= 12 && day >=1 && {
      ((month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 || month == 12) && day <= 31) ||
      ((month == 4 || month == 6 || month == 9 || month == 11) && day <= 30) ||
      (month == 2 && day <= 28) ||  (month == 2 && day == 29 && leapYear)
    }).option(unsafe(year, month, day))
  }

  def maxValue: Date =
    unsafe(3000.toShort, 12.toByte, 31.toByte)

  def minValue: Date =
    unsafe(1000.toShort, 1.toByte, 1.toByte)

  def unsafeFromInt(i: Int): Date =
    new Date(i)

  def fromInt(i: Int): Option[Date] =
    create(((i >>> 16) & 0xffff).toShort, ((i >>> 8) & 0xff).toByte, (i & 0xff).toByte)

  def fromLocalDate(d: LocalDate): Date =
    unsafe(d.getYear.toShort, d.getMonthOfYear.toByte, d.getDayOfMonth.toByte)

  def listParser: ListParser[Date] = {
    import ListParser._
    for {
      y        <- short
      m        <- short
      d        <- short
      result   <- create(y, m.toByte, d.toByte) match {
        case None => ListParser((position, _) => (position, s"""not a valid date ($y-$m-$d)""").failure)
        case Some(d) => d.point[ListParser]
      }
    } yield result
  }


  /**
   * This is not epoch! It will take a long which was created from Date.addSeconds and
   * pull the original Date and seconds out.
   */
  def fromSeconds(s: Long): Option[(Date, Int)] =
    fromInt(((s >>> 32) & 0xffffffff).toInt).map(_ -> (s & 0xffffffff).toInt)

  object Macros {
    import scala.reflect.macros.Context
    import language.experimental.macros

    def literal(c: Context)(year: c.Expr[Short], month: c.Expr[Byte], day: c.Expr[Byte]): c.Expr[Date] = {
      import c.universe._
      (year, month, day) match {
        case (Expr(Literal(Constant(y: Short))), Expr(Literal(Constant(m: Byte))), Expr(Literal(Constant(d: Byte)))) =>
          create(y, m, d) match {
            case None =>
              c.abort(c.enclosingPosition, s"This is not a valid date literal Date($y, $m, $d).")
            case Some(date) =>
              c.Expr(q"com.ambiata.ivory.core.Date.unsafe($y, $m, $d)")
          }
        case _ =>
          c.abort(c.enclosingPosition, s"Not a literal ${showRaw(year)}, ${showRaw(month)}, ${showRaw(day)}")
      }
    }
  }
}
