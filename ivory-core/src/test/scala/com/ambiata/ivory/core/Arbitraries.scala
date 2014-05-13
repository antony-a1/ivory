package com.ambiata.ivory.core

import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._

object Arbitraries {
  implicit def PriorityArbitrary: Arbitrary[Priority] =
    Arbitrary(arbitrary[Short] filter (_ >= 0) map (Priority.unsafe))

  implicit def DateArbitrary: Arbitrary[Date] =
    Arbitrary(for {
      y <- Gen.choose(1000, 3000)
      m <- Gen.choose(1, 12)
      d <- Gen.choose(1, 31)
      r = Date.create(y.toShort, m.toByte, d.toByte)
      if r.isDefined
    } yield r.get)

  implicit def TimeArbitrary: Arbitrary[Time] =
    Arbitrary(Gen.frequency(
      3 -> Gen.const(Time(0))
    , 1 -> Gen.choose(0, (60 * 60 * 24) - 1).map(Time.unsafe)
    ))

  implicit def DateTimeArbitrary: Arbitrary[DateTime] =
    Arbitrary(for {
      d <- arbitrary[Date]
      t <- arbitrary[Time]
    } yield d.addTime(t))

  lazy val TestDictionary: Dictionary = Dictionary("EavtParsersSpec", Map(
    FeatureId("fruit", "apple") -> FeatureMeta(LongEncoding, ContinuousType, "Reds and Greens", "?" :: Nil)
  , FeatureId("fruit", "orange") -> FeatureMeta(StringEncoding, CategoricalType, "Oranges", "?" :: Nil)
  , FeatureId("vegetables", "potatoe") -> FeatureMeta(DoubleEncoding, ContinuousType, "Browns", "?" :: Nil)
  ))

  lazy val TestEntities: List[String] =
    (1 to 10000).toList.map(i => "T+%05d".format(i))

  def valueOf(encoding: Encoding): Gen[Value] = encoding match {
    case BooleanEncoding =>
      arbitrary[Boolean].map(BooleanValue)
    case IntEncoding =>
      arbitrary[Int].map(IntValue)
    case LongEncoding =>
      arbitrary[Long].map(LongValue)
    case DoubleEncoding =>
      arbitrary[Double].map(DoubleValue)
    case StringEncoding =>
      arbitrary[String].map(StringValue)
   }

  implicit def FactArbitrary: Arbitrary[Fact] = Arbitrary(for {
    e <- Gen.oneOf(TestEntities)
    (f, m) <- Gen.oneOf(TestDictionary.meta.toList)
    d <- arbitrary[Date]
    t <- arbitrary[Time]
    v <- Gen.frequency(1 -> Gen.const(TombstoneValue()), 99 -> valueOf(m.encoding))
  } yield Fact.newFact(e, f.namespace, f.name, d, t, v))
}
