package com.ambiata.ivory.core

object DateMap {
  import org.joda.time.LocalDate
  import java.util.HashMap
  type Danger = HashMap[String, Array[Int]]

  def keep(dates: Danger, entity: String, year: Short, month: Byte, day: Byte): Boolean = {
    val x = dates.get(entity)
    x != null && toInt(year, month, day) <= x(0)
  }

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

  @inline def localDateToInt(d: LocalDate): Int =
    toInt(d.getYear.toShort, d.getMonthOfYear.toByte, d.getDayOfMonth.toByte)

  @inline def localDateFromInt(date: Int): LocalDate = {
    val (y, m, d) = fromInt(date)
    new LocalDate(y.toInt, m.toInt, d.toInt)
  }

  @inline def intToStringDate(date: Int): String = {
    val (y, m, d) = fromInt(date)
    "%4d-%02d-%02d".format(y, m, d)
  }
}


object BuildDanger extends App {
  import java.io._
  val block = 4 * 1024 * 1024
  val in = new BufferedInputStream(new FileInputStream("target/chord.in"), block)
  val s = new String(com.ambiata.mundane.io.Streams.readFromStream(in, block), "UTF-8")
  val c = DateMap.chords(s)
  println(c.size)
  println("keep: " + DateMap.keep(c, "E000001", 2013.toShort, 1.toByte, 1.toByte))
  println("reject: " + DateMap.keep(c, "E000001", 2015.toShort, 1.toByte, 1.toByte))
}

object BuildChordFile extends App {
  import java.io._
  val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("target/chord.in")))
  val rand = new java.util.Random
  def month = String.format("%02d", java.lang.Integer.valueOf((math.abs(rand.nextInt) % 12) + 1))
  def day = String.format("%02d", java.lang.Integer.valueOf((math.abs(rand.nextInt) % 28) + 1))

  (1 to 1000).foreach(n => {
    out.write(s"E00000$n|2014-${month}-${day}")
    out.newLine
  })
  out.flush
  out.close
}
