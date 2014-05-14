package com.ambiata.ivory.core

import com.ambiata.ivory.core.Arbitraries._
import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._
import org.joda.time._, format.DateTimeFormat
import scalaz._, Scalaz._

class DatesSpec extends Specification with ScalaCheck { def is = s2"""

Date Parsing
------------

  Symmetric                                       $symmetric
  Invalid year                                    $year
  Invalid month                                   $month
  Invalid day                                     $day
  Edge cases                                      $edge
  Exceptional - non numeric values                $exceptional
  Round-trip with joda                            $joda
  Parses same as joda                             $jodaparse


"""

  def symmetric = prop((d: Date) =>
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
}
