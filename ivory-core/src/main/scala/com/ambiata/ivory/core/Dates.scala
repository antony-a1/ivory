package com.ambiata.ivory.core
import scalaz._, Scalaz._
import org.joda.time.LocalDate
import com.ambiata.mundane.parse.ListParser

object Dates {
  def parse(s: String): Date \/ DateTime =
    ???

  // FIX It would be nice if we could avoid the joda overhead here, but we would need to
  //     implement something to handle daylight savings offsets before we could.
  def datetime(s: String): Option[DateTime] =
    try {
      ???
//      val d = DateTimeFormat.forPattern("yyyy-MM-ddTHH:mm:ss").parseLocalDateTime(s)
//
//      DateTime.create(d.getYear.toShort, d.getMonthOfYear.toByte, d.getDayOfMonth.toByte, d.getMillisOfDay)
    } catch { case e: Throwable => None }

  def date(s: String): Option[Date] =
    if (s.length != 10 || s.charAt(4) != '-' || s.charAt(7) != '-')
      None
    else try {
      val y = s.substring(0, 4).toShort
      val m = s.substring(5, 7).toByte
      val d = s.substring(8, 10).toByte
      if (Date.isValid(y, m, d)) Some(Date.unsafe(y, m, d)) else None
    } catch { case e: Throwable => None }
}
