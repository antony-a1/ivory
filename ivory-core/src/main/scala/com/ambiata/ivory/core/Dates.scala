package com.ambiata.ivory.core

import scalaz._, Scalaz._
import org.joda.time.{LocalDate, DateTimeZone, DateTime => JodaDateTime}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import com.ambiata.mundane.parse.ListParser

object Dates {
  def parse(s: String, local: DateTimeZone, ivory: DateTimeZone): Option[Date \/ DateTime] =
    if (s.length == 10)
      date(s).map(_.left)
    else if (s.length == 19)
      (datetime(s, local, ivory) orElse legacy(s, local, ivory)).map(_.right)
    else
      datetimezone(s, ivory).map(_.right)

  def date(s: String): Option[Date] =
    if (s.length != 10 || s.charAt(4) != '-' || s.charAt(7) != '-')
      None
    else try {
      val y = s.substring(0, 4).toShort
      val m = s.substring(5, 7).toByte
      val d = s.substring(8, 10).toByte
      if (Date.isValid(y, m, d)) Some(Date.unsafe(y, m, d)) else None
    } catch { case e: Throwable => None }

  def datetimezone(s: String, ivory: DateTimeZone): Option[DateTime] =
    joda(s, DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ"), ivory)

  def datetime(s: String, local: DateTimeZone, ivory: DateTimeZone): Option[DateTime] =
    joda(s, DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(local), ivory)

  def legacy(s: String, local: DateTimeZone, ivory: DateTimeZone): Option[DateTime] =
    joda(s, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(local), ivory)

  def joda(s: String, format: DateTimeFormatter, ivory: DateTimeZone): Option[DateTime] =
    try {
      val d = format.parseDateTime(s).withZone(ivory)
      DateTime.create(d.getYear.toShort, d.getMonthOfYear.toByte, d.getDayOfMonth.toByte, d.getSecondOfDay)
    } catch { case e: Throwable => None }

  def parser(local: DateTimeZone, ivory: DateTimeZone): ListParser[Date \/ DateTime] = {
    import ListParser._
    for {
      s <- string
      p <- getPosition
      r <- value(parse(s, local, ivory).map(_.success).getOrElse(s"Could not parse '${s}' at position '${p}'".failure))
    } yield r
  }

  def dst(year: Short, zone: DateTimeZone): Option[(DateTime, DateTime)] = {
    val endOfYear = new LocalDate(year + 1, 1, 1).toDateTimeAtStartOfDay(zone)
    val secondDst = new JodaDateTime(zone.previousTransition(endOfYear.getMillis), zone)
    val firstDst = new JodaDateTime(zone.previousTransition(secondDst.getMillis), zone)
    if(firstDst != endOfYear && secondDst != firstDst)
      Some((DateTime.fromJoda(firstDst), DateTime.fromJoda(secondDst)))
    else
      None
  }
}
