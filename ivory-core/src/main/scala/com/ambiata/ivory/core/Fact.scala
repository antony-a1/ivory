package com.ambiata.ivory.core

import scalaz._, Scalaz._
import com.ambiata.mundane.parse._

import com.ambiata.ivory.core.thrift._

trait Fact {
  def entity: String
  def namespace: String
  def feature: String
  def featureId: FeatureId
  def date: Date
  def time: Time
  def datetime: DateTime
  def value: Value
  def toThrift: ThriftFact

  def toNamespacedThrift: NamespacedThriftFact with NamespacedThriftFactDerived

  lazy val stringValue: Option[String] =
    value.stringValue

  def coordinateString(delim: Char): String = {
    val fields = List(s"$entity", s"$featureId", s"${date.int}-${time}}")
    fields.mkString(delim.toString)
  }

  def isTombstone: Boolean = value match {
    case v: TombstoneValue => true
    case _                  => false
  }

  def withEntity(newEntity: String): Fact =
    Fact.newFact(newEntity, namespace, feature, date, time, value)

  def withFeatureId(newFeatureId: FeatureId): Fact =
    Fact.newFact(entity, namespace, feature, date, time, value)

  def withDate(newDate: Date): Fact =
    Fact.newFact(entity, namespace, feature, newDate, time, value)

  def withTime(newTime: Time): Fact =
    Fact.newFact(entity, namespace, feature, date, newTime, value)

  def withValue(newValue: Value): Fact =
    Fact.newFact(entity, namespace, feature, date, time, newValue)
}

object Fact {
  def newFact(entity: String, namespace: String, feature: String, date: Date, time: Time, value: Value): Fact =
    FatThriftFact.factWith(entity, namespace, feature, date, time, value match {
      case StringValue(s)   => ThriftFactValue.s(s)
      case BooleanValue(b)  => ThriftFactValue.b(b)
      case IntValue(i)      => ThriftFactValue.i(i)
      case LongValue(l)     => ThriftFactValue.l(l)
      case DoubleValue(d)   => ThriftFactValue.d(d)
      case TombstoneValue() => ThriftFactValue.t(new ThriftTombstone())
    })
}

trait NamespacedThriftFactDerived extends Fact { self: NamespacedThriftFact  =>
    def namespace =
      nspace

    def feature =
      fact.attribute

    def date =
      Date.unsafeFromInt(yyyyMMdd)

    def time =
      Time.unsafe(seconds)

    def datetime =
      date.addTime(time)

    def entity: String =
      fact.getEntity

    def featureId: FeatureId =
      FeatureId(nspace, fact.getAttribute)

    def seconds: Int =
      Option(fact.getSeconds).getOrElse(0)

    def value: Value = fact.getValue match {
      case tv if(tv.isSetD) => DoubleValue(tv.getD)
      case tv if(tv.isSetS) => StringValue(tv.getS)
      case tv if(tv.isSetI) => IntValue(tv.getI)
      case tv if(tv.isSetL) => LongValue(tv.getL)
      case tv if(tv.isSetB) => BooleanValue(tv.getB)
      case tv if(tv.isSetT) => TombstoneValue()
      case _                => sys.error(s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${fact.toString}].'")
    }

    def toThrift = fact

    def toNamespacedThrift = this
}

object FatThriftFact {
  def apply(ns: String, date: Date, tfact: ThriftFact): Fact = new NamespacedThriftFact(tfact, ns, date.int) with NamespacedThriftFactDerived

  def factWith(entity: String, namespace: String, feature: String, date: Date, time: Time, value: ThriftFactValue): Fact = {
    val tfact = new ThriftFact(entity, feature, value)
    FatThriftFact(namespace, date, tfact.setSeconds(time.seconds))
  }
}

object BooleanFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Boolean): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.b(value))

  val fromTuple = apply _ tupled
}

object IntFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Int): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.i(value))

  val fromTuple = apply _ tupled
}

object LongFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Long): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.l(value))

  val fromTuple = apply _ tupled
}

object DoubleFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Double): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.d(value))

  val fromTuple = apply _ tupled
}

object StringFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: String): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.s(value))

  val fromTuple = apply _ tupled
}

object TombstoneFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.t(new ThriftTombstone()))

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
