package com.ambiata.ivory.core

import com.ambiata.ivory.core.Arbitraries._
import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._
import org.joda.time._, format.DateTimeFormat
import scalaz._, Scalaz._

class DatesSpec extends Specification with ScalaCheck { def is = s2"""

Date Parsing
------------

  Symmetric                                       $datesymmetric
  Invalid year                                    $year
  Invalid month                                   $month
  Invalid day                                     $day
  Edge cases                                      $edge
  Exceptional - non numeric values                $exceptional
  Round-trip with joda                            $joda
  Parses same as joda                             $jodaparse

Date Time Parsing
-----------------

  Symmteric                                       $timesymmetric

Date Time Parsing w/ Zones
--------------------------

  Symmteric                                       $zonesymmetric

Generic Time Format Parsing
---------------------------

  Dates are recognized                            $parsedate
  Date/Times are recognized                       $parsetime
  Date/Time/Zones are recognized                  $parsezone
  Everything else fails                           $parsefail

"""

  def datesymmetric = prop((d: Date) =>
    Dates.date(d.hyphenated) must beSome(d))

  def year = prop((d: Date) =>
    Dates.date("0100-%02d-%02d".format(d.month, d.day)) must beNone)

  def month = prop((d: Date) =>
    Dates.date("%4d-13-%02d".format(d.year, d.day)) must beNone)

  def day = prop((d: Date) =>
    Dates.date("%4d-%02d-32".format(d.year, d.month)) must beNone)

  def exceptional = prop((d: Date) =>
    Dates.date(d.hyphenated.replaceAll("""\d""", "x")) must beNone)

  def joda = prop((d: Date) =>
    Dates.date(new LocalDate(d.year, d.month, d.day).toString("yyyy-MM-dd")) must beSome(d))

  def jodaparse = prop((d: Date) => {
    val j = DateTimeFormat.forPattern("yyyy-MM-dd").parseLocalDate(d.hyphenated)
    (j.getYear, j.getMonthOfYear, j.getDayOfMonth) must_== ((d.year.toInt, d.month.toInt, d.day.toInt)) })

  def edge = {
    (Dates.date("2000-02-29") must beSome(Date(2000, 2, 29))) and
    (Dates.date("2001-02-29") must beNone)
  }

  /**
   * TODO: Fix when we handle DST
   */
  def zonesymmetric = prop((d: DateTime, local: DateTimeZone, ivory: DateTimeZone) => runExample(d, local, ivory) ==> {
    Dates.datetimezone(d.iso8601(local), ivory) must beSome((iDate: DateTime) => {
      val jdt = iDate.joda(ivory).withZone(local)
      DateTime.unsafe(jdt.getYear.toShort, jdt.getMonthOfYear.toByte, jdt.getDayOfMonth.toByte, jdt.getSecondOfDay.toInt) must_== d
    })
  }).set(minTestsOk = 50000)

  /**
   * TODO: Fix when we handle DST
   */
  def timesymmetric = prop((d: DateTime, local: DateTimeZone, ivory: DateTimeZone) => runExample(d, local, ivory) ==> {
    Dates.datetime(d.localIso8601, local, ivory) must beSome((iDate: DateTime) => {
      val jdt = iDate.joda(ivory).withZone(local)
      DateTime.unsafe(jdt.getYear.toShort, jdt.getMonthOfYear.toByte, jdt.getDayOfMonth.toByte, jdt.getSecondOfDay.toInt) must_== d
    })
  }).set(minTestsOk = 50000)

  def runExample(d: DateTime, local: DateTimeZone, ivory: DateTimeZone): Boolean =
    try {
      d.joda(local)
      !d.isDstOverlap(local) &&
      !DateTime.fromJoda(d.joda(local).withZone(ivory)).isDstOverlap(ivory) &&
      (local.toString != "Africa/Monrovia" || d.date.year > 1980) // for some reason there are issues with this specific timezone before 1980
    } catch {
      case e: java.lang.IllegalArgumentException => false
    }

  def parsedate = prop((d: DateTime, ivory: DateTimeZone) =>
    Dates.parse(d.date.hyphenated, ivory, ivory) must beSome((nd: Date \/ DateTime) => nd.toEither must beLeft(d.date)))

  def parsetime = prop((d: DateTime, ivory: DateTimeZone) =>
    Dates.parse(d.localIso8601, ivory, ivory) must beSome((nd: Date \/ DateTime) => nd.toEither must beRight(d)))

  def parsezone = prop((d: DateTime, ivory: DateTimeZone) =>
    Dates.parse(d.iso8601(ivory), ivory, ivory) must beSome((nd: Date \/ DateTime) => nd.toEither must beRight(d)))

  def parsefail =
    (Dates.parse("2001-02-29", DateTimeZone.UTC, DateTimeZone.UTC) must beNone) and
    (Dates.parse("2001-02-20T25:10:01", DateTimeZone.UTC, DateTimeZone.UTC) must beNone) and
    (Dates.parse("2001-02-20T20:10:01-24:00", DateTimeZone.UTC, DateTimeZone.UTC) must beNone)

}
