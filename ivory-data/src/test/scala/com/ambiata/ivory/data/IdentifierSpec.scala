package com.ambiata.ivory.data

import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._

class IdentiferSpec extends Specification with ScalaCheck { def is = s2"""

Identifier Properties
---------------------

  Can't overflow                                  $overflow
  Can't parse longs                               $longs
  Can parse all ints                              $ints
  Render/parse is symmetric                       $symmetric
  Initial starts with zero                        $initial
  If next succeeds identifier is always larger    $next
  Literals work                                   $literals

"""
  def overflow = {
    val max = Identifier.parse("ffffffff")
    (max must beSome) and (max.flatMap(_.next) must beNone)
  }

  def longs =
    Identifier.parse("fffffffff") must beNone

  def ints = prop((n: Int) =>
    Identifier.parse(String.format("%08x", java.lang.Integer.valueOf(n))) must beSome)

  def symmetric = prop((i: Identifier) =>
    Identifier.parse(i.render) must_== Some(i))

  def initial =
    Some(Identifier.initial) must_== Identifier.parse("0")

  def next = prop((i: Identifier) =>
    i.next.forall(_ > i))

  def literals = {
    import IvoryDataLiterals._
    Some(i"1234") must_== Identifier.parse("1234")
  }

  implicit def IdentifierArbitrary: Arbitrary[Identifier] =
    Arbitrary(Gen.choose(0, 200) map (n => (1 to n).foldLeft(Identifier.initial)((acc, _) => acc.next.get)))
}
