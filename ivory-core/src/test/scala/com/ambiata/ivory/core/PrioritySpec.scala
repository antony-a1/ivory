package com.ambiata.ivory.core

import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._

class PrioritySpec extends Specification with ScalaCheck { def is = s2"""

Date Tests
----------

  Priority order is isomorphic to Short order       $order
  Literals (compilation test)                       $ok

"""
  implicit def PriorityArbitrary: Arbitrary[Priority] =
    Arbitrary(arbitrary[Short] filter (_ >= 0) map (Priority.unsafe))

  def order = prop((a: Priority, b: Priority) =>
    (a.toShort ?|? b.toShort) must_== (a ?|? b))

  Priority(0)
  Priority(100)
  Priority(Short.MaxValue)
}
