package com.ambiata.ivory
package scoobi

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import java.io._
import org.joda.time.LocalDate

import core._

trait WireFormats {

  implicit def FactFmt = new WireFormat[Fact] {
    def toWire(f: Fact, out: DataOutput) = {
      StringFmt.toWire(f.entity, out)
      StringFmt.toWire(f.featureId.namespace, out)
      StringFmt.toWire(f.featureId.name, out)
      LocalDateFmt.toWire(f.date, out)
      IntFmt.toWire(f.seconds, out)
      ValueFmt.toWire(f.value, out)
    }
    
    def fromWire(in: DataInput): Fact =
      Fact(StringFmt.fromWire(in),
           FeatureId(StringFmt.fromWire(in), StringFmt.fromWire(in)),
           LocalDateFmt.fromWire(in),
           IntFmt.fromWire(in),
           ValueFmt.fromWire(in))
  }

  implicit def BooleanValueFmt = new WireFormat[BooleanValue] {
    def toWire(v: BooleanValue, out: DataOutput) =
      BooleanFmt.toWire(v.value, out)    
    def fromWire(in: DataInput): BooleanValue =
      BooleanValue(BooleanFmt.fromWire(in))
  }

  implicit def IntValueFmt = new WireFormat[IntValue] {
    def toWire(v: IntValue, out: DataOutput) =
      IntFmt.toWire(v.value, out)
    def fromWire(in: DataInput): IntValue =
      IntValue(IntFmt.fromWire(in))
  }

  implicit def LongValueFmt = new WireFormat[LongValue] {
    def toWire(v: LongValue, out: DataOutput) =
      LongFmt.toWire(v.value, out)
    def fromWire(in: DataInput): LongValue =
      LongValue(LongFmt.fromWire(in))
  }

  implicit def DoubleValueFmt = new WireFormat[DoubleValue] {
    def toWire(v: DoubleValue, out: DataOutput) =
      DoubleFmt.toWire(v.value, out)
    def fromWire(in: DataInput): DoubleValue =
      DoubleValue(DoubleFmt.fromWire(in))
  }

  implicit def StringValueFmt = new WireFormat[StringValue] {
    def toWire(v: StringValue, out: DataOutput) =
      StringFmt.toWire(v.value, out)
    def fromWire(in: DataInput): StringValue =
      StringValue(StringFmt.fromWire(in))
  }

  implicit def TombstoneValueFmt = new WireFormat[TombstoneValue] {
    def toWire(v: TombstoneValue, out: DataOutput) = ()
    def fromWire(in: DataInput): TombstoneValue = TombstoneValue()
  }

  implicit def ValueFmt = mkAbstractWireFormat[core.Value, BooleanValue, IntValue, LongValue, DoubleValue, StringValue, TombstoneValue]

  implicit def ValidationFmt[A, B](implicit awf: WireFormat[A], bwf: WireFormat[B]) = new WireFormat[Validation[A, B]] {
    def toWire(v: Validation[A, B], out: DataOutput) = {
      v match {
        case Failure(a) => { out.writeBoolean(false); awf.toWire(a, out) }
        case Success(b) => { out.writeBoolean(true); bwf.toWire(b, out) }
      }
    }
  
    def fromWire(in: DataInput): Validation[A, B] = {
      in.readBoolean match {
        case false => awf.fromWire(in).failure
        case true  => bwf.fromWire(in).success
      }
    }

    def show(v: Validation[A, B]): String = v.toString
  }

  implicit def DisjunctionFmt[A, B](implicit wf: WireFormat[Either[A, B]]) = new WireFormat[A \/ B] {
    def toWire(v: A \/ B, out: DataOutput) = wf.toWire(v.toEither, out)
    def fromWire(in: DataInput): A \/ B = wf.fromWire(in).disjunction
    def show(v: A \/ B): String = v.toString
  }

  implicit def LocalDateFmt = new WireFormat[LocalDate] {
    def toWire(x: LocalDate, out: DataOutput) = { out.writeShort(x.getYear); out.writeShort(x.getMonthOfYear); out.writeShort(x.getDayOfMonth) }
    def fromWire(in: DataInput): LocalDate = new LocalDate(in.readShort.toInt, in.readShort.toInt, in.readShort.toInt)
    override def toString = "LocalDate"
  }

  def mkThriftFmt[A](empty: A)(implicit ev: A <:< org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]) = new WireFormat[A] {
    val serialiser = new org.apache.thrift.TSerializer(new org.apache.thrift.protocol.TCompactProtocol.Factory)
    val deserialiser = new org.apache.thrift.TDeserializer(new org.apache.thrift.protocol.TCompactProtocol.Factory)
    val buffer = new ByteArrayOutputStream()

    def toWire(x: A, out: DataOutput) = {
      out.write(serialiser.serialize(ev(x)))
    }

    def fromWire(in: DataInput) = {
      buffer.reset()
      val e = ev(empty).deepCopy
      e.clear
      while(try { buffer.write(in.readByte()); true } catch { case e: EOFException => false }) ()
      deserialiser.deserialize(e, buffer.toByteArray)
      e.asInstanceOf[A]
    }
    override def toString = "ThriftObject"
  }
}

object WireFormats extends WireFormats
