package com.ambiata.ivory.scoobi

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.core.thrift.ThriftSerialiser
import com.nicta.scoobi.Scoobi._

import org.apache.hadoop.io.BytesWritable

object SeqSchemas {

  def mkThriftSchema[A](empty: A)(implicit ev: A <:< org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]) = new SeqSchema[A] {
    type SeqType = BytesWritable
    val serialiser = ThriftSerialiser()
    def toWritable(x: A) = new BytesWritable(serialiser.toBytes(x))
    def fromWritable(x: BytesWritable): A = serialiser.fromBytes(empty, x.getBytes)
    val mf: Manifest[SeqType] = implicitly
  }

  val ThriftFactSeqSchema: SeqSchema[ThriftFact] =
    mkThriftSchema(new ThriftFact)

  val NamespacedThiftFactSeqSchema: SeqSchema[NamespacedThriftFact] =
    mkThriftSchema(new NamespacedThriftFact)

  val FactSeqSchema: SeqSchema[Fact] = new SeqSchema[Fact] {
    type SeqType = BytesWritable
    val empty = new NamespacedThriftFact with NamespacedThriftFactDerived
    val serialiser = ThriftSerialiser()
    def toWritable(x: Fact) = new BytesWritable(serialiser.toBytes(x.toNamespacedThrift))
    def fromWritable(x: BytesWritable): Fact = serialiser.fromBytes(empty, x.getBytes)
    val mf: Manifest[SeqType] = implicitly
  }
}
