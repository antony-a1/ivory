package com.ambiata.ivory.scoobi

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, Value => _, _}, Scalaz._
import java.io._
import org.joda.time.LocalDate

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._

import scala.collection.JavaConverters._

trait WireFormats {

  /**
   * TODO Remove this when scoobi has the wire format
   */
  implicit val ShortWireFormat = new WireFormat[Short] {
    def toWire(x: Short, out: DataOutput) { out.writeShort(x) }
    def fromWire(in: DataInput): Short = in.readShort()
    override def toString = "Short"
  }

  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  /* this is a special snowflake because you want to mix in Fact without the overhead of creating two objects. */
  def factWireFormat = new WireFormat[Fact] {
    import org.apache.thrift.protocol.TCompactProtocol
    import org.apache.thrift.{TBase, TFieldIdEnum, TSerializer, TDeserializer}

    def toWire(x: Fact, out: DataOutput) = {
      val serialiser = new TSerializer(new TCompactProtocol.Factory)
      val bytes = serialiser.serialize(x.toNamespacedThrift)
      out.writeInt(bytes.length)
      out.write(bytes)
    }
    def fromWire(in: DataInput): Fact = {
      val deserialiser = new TDeserializer(new TCompactProtocol.Factory)
      val size = in.readInt()
      val bytes = new Array[Byte](size)
       in.readFully(bytes)
      val e = new NamespacedThriftFact with NamespacedThriftFactDerived
      deserialiser.deserialize(e, bytes)
      e
    }
  }

  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  def namespacedThriftFactWireFormat = new WireFormat[NamespacedThriftFact] {
    val x = mkThriftFmt(new NamespacedThriftFact)
    def toWire(tf: NamespacedThriftFact, out: DataOutput) =  x.toWire(tf, out)
    def fromWire(in: DataInput): NamespacedThriftFact = x.fromWire(in)
  }

  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  def thriftFactWireFormat = new WireFormat[ThriftFact] {
    val x = mkThriftFmt(new ThriftFact)
    def toWire(tf: ThriftFact, out: DataOutput) = x.toWire(tf, out)
    def fromWire(in: DataInput): ThriftFact = x.fromWire(in)
  }

  implicit def ValidationWireFormat[A, B](implicit awf: WireFormat[A], bwf: WireFormat[B]) = new WireFormat[Validation[A, B]] {
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

  implicit def DisjunctionWireFormat[A, B](implicit wf: WireFormat[Either[A, B]]) = new WireFormat[A \/ B] {
    def toWire(v: A \/ B, out: DataOutput) = wf.toWire(v.toEither, out)
    def fromWire(in: DataInput): A \/ B = wf.fromWire(in).disjunction
    def show(v: A \/ B): String = v.toString
  }

  implicit def DateMapWireFormat = AnythingFmt[java.util.HashMap[String, Array[Int]]]

  /* WARNING THIS IS NOT SAFE TO EXPOSE, DANGER LURKS, SEE ThriftFactWireFormat */
  private def mkThriftFmt[A](empty: A)(implicit ev: A <:< org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]): WireFormat[A] = new WireFormat[A] {
    val serialiser = ThriftSerialiser()
    def toWire(x: A, out: DataOutput) = {
      val bytes = serialiser.toBytes(x)
      out.writeInt(bytes.length)
      out.write(bytes)
    }
    def fromWire(in: DataInput): A = {
      val size = in.readInt()
      val bytes = new Array[Byte](size)
      in.readFully(bytes)
      serialiser.fromBytes(empty, bytes)
    }
    override def toString = "ThriftObject"
  }
}

object WireFormats extends WireFormats
