package com.ambiata.ivory.storage.parse

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._

import org.joda.time.DateTimeZone
import org.scalacheck._, Arbitrary._
import org.specs2._, matcher._, specification._

import scalaz._, Scalaz._

class EavtParsersSpec extends Specification with ScalaCheck { def is = s2"""

Eavt Parse Formats
------------------

 Can parse date only                     $date
 Can parse legacy date-time format       $legacy
 Can parse standard date-time format     $standard
 Can parse with different time zones     $zones

"""
  def date = prop((fact: Fact) =>
    EavtParsers.fact(TestDictionary, fact.namespace, DateTimeZone.getDefault).run(List(
      fact.entity
    , fact.feature
    , fact.value.stringValue.getOrElse("?")
    , fact.date.hyphenated
    )) must_== Success(fact.withTime(Time(0))))

  def legacy = prop((fact: Fact) =>
    EavtParsers.fact(TestDictionary, fact.namespace, DateTimeZone.getDefault).run(List(
      fact.entity
    , fact.feature
    , fact.value.stringValue.getOrElse("?")
    , fact.date.hyphenated + " " + fact.time.hhmmss
    )) must_== Success(fact)).pendingUntilFixed

  // FIX need to add support for a more sensible 8601 format, see /doc/dates.md
  def standard =
    pending

  def zones = prop((fact: Fact) =>
    EavtParsers.fact(TestDictionary, fact.namespace, DateTimeZone.forID("Indian/Maldives")).run(List(
      fact.entity
    , fact.feature
    , fact.value.stringValue.getOrElse("?")
    , fact.date.hyphenated + " " + fact.time.hhmmss
    )) must_== Success(fact)).pendingUntilFixed
}
