package com.ambiata.ivory.core

import scala.collection.JavaConverters._

// FIX deprecate in favour of Date/DateTime operations.
object DateMap {
  import org.joda.time.LocalDate
  import java.util.HashMap
  type Danger = HashMap[String, Array[Int]]

  def keep(dates: Danger, entity: String, year: Short, month: Byte, day: Byte): Boolean = {
    val x = dates.get(entity)
    x != null && toInt(year, month, day) <= x(0)
  }

  /**
   * Get the earliest and latest date (respectively)
   */
  def bounds(dates: Danger): (Date, Date) =
    dates.values.asScala.foldLeft((Date.maxValue, Date.minValue))({ case ((lmin, lmax), ds) =>
      val min = Date.unsafeFromInt(ds.min)
      val max = Date.unsafeFromInt(ds.max)
      (if(min isBefore lmin) min else lmin, if(max isAfter lmax) max else lmax)
    })

  def earliest(dates: Danger): Date =
    bounds(dates)._1

  def latest(dates: Danger): Date =
    bounds(dates)._2

  def chords(s: String): Danger = {
    val Date = """(\d{4})-(\d{2})-(\d{2})""".r
    val lines = s.lines.toList
    val out = new HashMap[String, Array[Int]](lines.length)
    lines.map(_.split("\\|").toList match {
      case h :: Date(y, m, d) :: Nil =>
        (h, y.toShort, m.toByte, d.toByte)
    }).groupBy(_._1).foreach({ case (k, v) =>
      out.put(k, v.map({ case (_, y, m, d) => toInt(y, m, d) }).toArray.sorted.reverse)
    })
    out
  }

  @inline def fromInt(d: Int): (Short, Byte, Byte)  =
    (((d >>> 16) & 0xffff).toShort, ((d >>> 8) & 0xff).toByte, (d & 0xff).toByte)

  @inline def toInt(y: Short, m: Byte, d: Byte)  =
    (y.toInt << 16) | (m.toInt << 8) | d.toInt
}
