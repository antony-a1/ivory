package com.ambiata.ivory.core

import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._

class SkewSpec extends Specification with ScalaCheck { def is = s2"""

Skew Tests
----------

  If everything is smaller than optimal, number of allocate the same number of reducers as namespaces
         $small

  If a namespace is larger than optimal _and_ there is more than one feature in that namespace, that
  namespace should be allocated to more than one reducer.
         $spread

  If a namespace is larger than features * optimal, every feature should be in its assigned its own
  reducer.
         $large

"""
   def small = {
     val (alloc, r) = Skew.calculate(dict, List(
       "demographics" -> 1L
     , "offers" -> 1L
     , "widgets" -> 1L
     , "knobs" -> 1L
     , "flavours" -> 1L
     ), optimal)
     alloc must_== 5
   }

  def spread = {
    val (alloc, r) = Skew.calculate(dict, sizes, optimal)
    sizes.forall({ case (n, size) =>
      val all = r.filter(_._1 == n)
      val reducers = all.map(_._3).distinct.size
      if (size > optimal && dict.forNamespace(n).meta.size > 1)
        reducers > 1
      else
        reducers == 1
    })
  }

  def large = {
    val (alloc, r) = Skew.calculate(dict, sizes, optimal)
    sizes.filter({ case (n, size) =>
      size > (dict.forNamespace(n).meta.size.toLong * optimal.toLong)
    }).forall({ case (n, size) =>
      val all = r.filter(_._1 == n)
      all.map(_._3).distinct.size must_== all.size
    })
  }

  def sizes = List(
    "demographics" -> 25986865L
  , "offers" -> 57890389L
  , "widgets" -> 329028927L
  , "knobs" -> 8380852917L
  , "flavours" -> 184072795L
  )
  def optimal = 1024 * 1024 * 256 // 256MB
  def fake = FeatureMeta(DoubleEncoding, ContinuousType, "desc", Nil)
  def featureIds =
    (1 to 10).map(n => FeatureId("demographics", "d" + n)).toList ++
    (1 to 10).map(n => FeatureId("offers", "o" + n)).toList ++
    (1 to 10).map(n => FeatureId("widgets", "w" + n)).toList ++
    (1 to 10).map(n => FeatureId("knobs", "k" + n)).toList ++
    (1 to 10).map(n => FeatureId("flavours", "f" + n)).toList
  def dict = Dictionary(featureIds.map(_ -> fake).toMap)
}
