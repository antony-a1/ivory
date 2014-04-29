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
      ((month == 4 || month == 6 || month == 9 || month == 11) && day <= 31) ||
      (month == 2 && day <= 28) ||  (month == 2 && day == 29 && leapYear)
    }).option(unsafe(year, month, day))
  }

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
        case None => ListParser((position, _) => s"""Not a valid date ($y-$m-$d) at position [$position]""".failure)
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



/* Seconds since start of day */
class Time private(val underlying: Int) extends AnyVal {
  def seconds =
    underlying

  override def toString: String =
    seconds.toString
}

object Time {
  def apply(seconds: Int): Time =
    macro Macros.literal

  def unsafe(seconds: Int): Time =
    new Time(seconds)

  def create(seconds: Int): Option[Time] =
    (seconds >= 0 && seconds < (60 * 60 * 24)).option(unsafe(seconds))

  object Macros {
    import scala.reflect.macros.Context
    import language.experimental.macros

    def literal(c: Context)(seconds: c.Expr[Int]): c.Expr[Time] = {
      import c.universe._
      seconds match {
        case Expr(Literal(Constant(s: Int))) =>
          create(s) match {
            case None =>
              c.abort(c.enclosingPosition, s"This is not a valid time literal Time($s).")
            case Some(date) =>
              c.Expr(q"com.ambiata.ivory.core.Time.unsafe($s)")
          }
        case _ =>
          c.abort(c.enclosingPosition, s"Not a literal ${showRaw(seconds)}")
      }
    }
  }

}

/* a packed long | 16 bits: year represented as a short | 8 bits: month represented as a byte | 8 bits: day represented as a byte | 32 bits: seconds since start of day */
class DateTime private(val underlying: Long) extends AnyVal {
  def date: Date =
    Date.unsafeFromInt((underlying >>> 32).toInt)

  def time: Time =
    Time.unsafe((underlying & 0xffffffff).toInt)

  def zip: (Date, Time) =
    date -> time

  def long: Long =
    underlying

  override def toString: String =
    s"DateTime(${date.year},${date.month},${date.day},$time)"
}

object DateTime {
  def apply(year: Short, month: Byte, day: Byte, seconds: Int): DateTime =
    macro Macros.literal

  def unsafe(year: Short, month: Byte, day: Byte, seconds: Int): DateTime =
    new DateTime(Date.unsafe(year, month, day).int.toLong << 32 | seconds)

  def create(year: Short, month: Byte, day: Byte, seconds: Int): Option[DateTime] = for {
    d <- Date.create(year, month, day)
    t <- Time.create(seconds)
  } yield new DateTime(d.int.toLong << 32 | t.seconds)

  def unsafeFromLong(l: Long): DateTime =
    new DateTime(l)

  object Macros {
    import scala.reflect.macros.Context
    import language.experimental.macros

    def literal(c: Context)(year: c.Expr[Short], month: c.Expr[Byte], day: c.Expr[Byte], seconds: c.Expr[Int]): c.Expr[DateTime] = {
      import c.universe._
      (year, month, day, seconds) match {
        case (Expr(Literal(Constant(y: Short))), Expr(Literal(Constant(m: Byte))), Expr(Literal(Constant(d: Byte))), Expr(Literal(Constant(s: Int)))) =>
          create(y, m, d, s) match {
            case None =>
              c.abort(c.enclosingPosition, s"This is not a valid date literal Date($y, $m, $d, $s).")
            case Some(date) =>
              c.Expr(q"com.ambiata.ivory.core.DateTime.unsafe($y, $m, $d, $s)")
          }
        case _ =>
          c.abort(c.enclosingPosition, s"Not a literal ${showRaw(year)}, ${showRaw(month)}, ${showRaw(day)}, ${showRaw(seconds)}")
      }
    }
  }
}
