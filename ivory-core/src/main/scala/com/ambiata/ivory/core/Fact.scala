package com.ambiata.ivory.core

import scalaz._, Scalaz._
import com.ambiata.mundane.parse._

import com.ambiata.ivory.core.thrift._

trait Fact {

  def entity: String
  def featureId: FeatureId
  def date: Date
  def seconds: Int
  def value: Value

  def toThrift: FatThriftFact

  /** This is not epoch, just a comparable long containing date and time */
  lazy val datetime: Long =
    date.addSeconds(seconds)

  lazy val stringValue: Option[String] =
    value.stringValue

  def coordinateString(delim: Char): String = {
    val fields = List(s"$entity", s"$featureId", s"${date.int}-${seconds}}")
    fields.mkString(delim.toString)
  }

  def isTombstone: Boolean = value match {
    case v: TombstoneValue => true
    case _                  => false
  }

  def withEntity(newEntity: String): Fact =
    Fact.newFact(newEntity, featureId, date, seconds, value)

  def withFeatureId(newFeatureId: FeatureId): Fact =
    Fact.newFact(entity, newFeatureId, date, seconds, value)

  def withDate(newDate: Date): Fact =
    Fact.newFact(entity, featureId, newDate, seconds, value)

  def withSeconds(newSeconds: Int): Fact =
    Fact.newFact(entity, featureId, date, newSeconds, value)

  def withValue(newValue: Value): Fact =
    Fact.newFact(entity, featureId, date, seconds, newValue)
}

object Fact {

  def newFact(entity: String, featureId: FeatureId, date: Date, seconds: Int, value: Value): Fact =
    FatThriftFact.factWith(entity, featureId, date, seconds, value match {
      case StringValue(s)   => ThriftFactValue.s(s)
      case BooleanValue(b)  => ThriftFactValue.b(b)
      case IntValue(i)      => ThriftFactValue.i(i)
      case LongValue(l)     => ThriftFactValue.l(l)
      case DoubleValue(d)   => ThriftFactValue.d(d)
      case TombstoneValue() => ThriftFactValue.t(new ThriftTombstone())
    })
}

case class FatThriftFact(ns: String, date: Date, tfact: ThriftFact) extends Fact {

  lazy val entity: String =
    tfact.getEntity

  lazy val featureId: FeatureId =
    FeatureId(ns, tfact.getAttribute)

  lazy val seconds: Int =
    Option(tfact.getSeconds).getOrElse(0)

  lazy val value: Value = tfact.getValue match {
    case tv if(tv.isSetS) => StringValue(tv.getS)
    case tv if(tv.isSetB) => BooleanValue(tv.getB)
    case tv if(tv.isSetI) => IntValue(tv.getI)
    case tv if(tv.isSetL) => LongValue(tv.getL)
    case tv if(tv.isSetD) => DoubleValue(tv.getD)
    case tv if(tv.isSetT) => TombstoneValue()
    case _                => sys.error(s"Something went really wrong, i found a thrift fact which i dont understand! - '${tfact.toString}'")
  }

  def toThrift = this
}

object FatThriftFact {

  def factWith(entity: String, featureId: FeatureId, date: Date, seconds: Int, value: => ThriftFactValue): FatThriftFact = {
    val tfact = new ThriftFact(entity, featureId.name, value)
    FatThriftFact(featureId.namespace, date, tfact.setSeconds(seconds))
  }
}

object BooleanFact {
  def apply(entity: String, featureId: FeatureId, date: Date, seconds: Int, value: Boolean): Fact =
    FatThriftFact.factWith(entity, featureId, date, seconds, ThriftFactValue.b(value))

  val fromTuple = apply _ tupled
}

object IntFact {
  def apply(entity: String, featureId: FeatureId, date: Date, seconds: Int, value: Int): Fact =
    FatThriftFact.factWith(entity, featureId, date, seconds, ThriftFactValue.i(value))

  val fromTuple = apply _ tupled
}

object LongFact {
  def apply(entity: String, featureId: FeatureId, date: Date, seconds: Int, value: Long): Fact =
    FatThriftFact.factWith(entity, featureId, date, seconds, ThriftFactValue.l(value))

  val fromTuple = apply _ tupled
}

object DoubleFact {
  def apply(entity: String, featureId: FeatureId, date: Date, seconds: Int, value: Double): Fact =
    FatThriftFact.factWith(entity, featureId, date, seconds, ThriftFactValue.d(value))

  val fromTuple = apply _ tupled
}

object StringFact {
  def apply(entity: String, featureId: FeatureId, date: Date, seconds: Int, value: String): Fact =
    FatThriftFact.factWith(entity, featureId, date, seconds, ThriftFactValue.s(value))

  val fromTuple = apply _ tupled
}

object TombstoneFact {
  def apply(entity: String, featureId: FeatureId, date: Date, seconds: Int): Fact =
    FatThriftFact.factWith(entity, featureId, date, seconds, ThriftFactValue.t(new ThriftTombstone()))

  val fromTuple = apply _ tupled
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
