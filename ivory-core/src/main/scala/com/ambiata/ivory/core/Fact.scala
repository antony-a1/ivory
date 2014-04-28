package com.ambiata.ivory.core

import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import com.ambiata.mundane.parse._

import scalaz._, Scalaz._

case class Fact(entity: String, featureId: FeatureId, date: LocalDate, seconds: Int, value: Value) {
  lazy val time: LocalDateTime =
    date.toDateTimeAtStartOfDay.toLocalDateTime.plusSeconds(seconds)

  lazy val epoch: Long =
    time.toDateTime.getMillis / 1000

  lazy val stringValue: Option[String] =
    value.stringValue

  def coordinateString(delim: Char): String = {
    val fields = List(s"$entity", s"$featureId", s"${time.toString("yyyy-MM-dd hh:mm:ss")}")
    fields.mkString(delim.toString)
  }

  def isTombstone: Boolean = value match {
    case v: TombstoneValue => true
    case _                  => false
  }
}

sealed trait Value {
  def encoding: Encoding
  def stringValue: Option[String]
}
case class BooleanValue(value: Boolean) extends Value {
  val encoding = BooleanEncoding
  val stringValue = Some(value.toString)
}
case class IntValue(value: Int) extends Value {
  val encoding = IntEncoding
  val stringValue = Some(value.toString)
}
case class LongValue(value: Long) extends Value {
  val encoding = LongEncoding
  val stringValue = Some(value.toString)
}
case class DoubleValue(value: Double) extends Value {
  val encoding = DoubleEncoding
  val stringValue = Some(value.toString)
}
case class StringValue(value: String) extends Value {
  val encoding = StringEncoding
  val stringValue = Some(value.toString)
}
case class TombstoneValue() extends Value {
  val encoding = TombstoneEncoding
  val stringValue = None
}

object BooleanFact {
  def apply(entity: String, featureId: FeatureId, date: LocalDate, seconds: Int, value: Boolean) =
    Fact(entity, featureId, date, seconds, BooleanValue(value))

  val fromTuple = apply _ tupled
}

object IntFact {
  def apply(entity: String, featureId: FeatureId, date: LocalDate, seconds: Int, value: Int) =
    Fact(entity, featureId, date, seconds, IntValue(value))

  val fromTuple = apply _ tupled
}

object LongFact {
  def apply(entity: String, featureId: FeatureId, date: LocalDate, seconds: Int, value: Long) =
    Fact(entity, featureId, date, seconds, LongValue(value))

  val fromTuple = apply _ tupled
}

object DoubleFact {
  def apply(entity: String, featureId: FeatureId, date: LocalDate, seconds: Int, value: Double) =
    Fact(entity, featureId, date, seconds, DoubleValue(value))

  val fromTuple = apply _ tupled
}

object StringFact {
  def apply(entity: String, featureId: FeatureId, date: LocalDate, seconds: Int, value: String) =
    Fact(entity, featureId, date, seconds, StringValue(value))

  val fromTuple = apply _ tupled
}

object TombstoneFact {
  def apply(entity: String, featureId: FeatureId, date: LocalDate, seconds: Int) =
    Fact(entity, featureId, date, seconds, TombstoneValue())

  val fromTuple = apply _ tupled
}
