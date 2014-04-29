package com.ambiata.ivory.core

import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._

class DateSpec extends Specification with ScalaCheck { def is = s2"""

Date Tests
-----------------------------

  Int dates are reversable                    $int
  Can add seconds to int date and reverse     $sec

"""
  def int = {
    val d = Date(2012, 10, 1)
    Date.fromInt(d.int) must_== d
  }

  def sec = prop((n: Int) => n < (24 * 60 * 60) ==> {
    val d = Date(2012, 10, 1)
    d.addTime(Time.unsafe(n)).zip must_== ((d, 1234))
  })
}
