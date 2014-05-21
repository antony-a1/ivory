package com.ambiata.ivory.scoobi

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.core.thrift.ThriftSerialiser
import com.nicta.scoobi.Scoobi._

import org.apache.hadoop.io.BytesWritable

object SeqSchemas {
  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  def thriftFactSeqSchema: SeqSchema[ThriftFact] =
    mkThriftSchema(new ThriftFact)

  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  def namespacedThiftFactSeqSchema: SeqSchema[NamespacedThriftFact] =
    mkThriftSchema(new NamespacedThriftFact)

  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  def factSeqSchema: SeqSchema[Fact] = new SeqSchema[Fact] {
    type SeqType = BytesWritable
    def empty = new NamespacedThriftFact with NamespacedThriftFactDerived
    val serialiser = ThriftSerialiser()
    def toWritable(x: Fact) = new BytesWritable(serialiser.toBytes(x.toNamespacedThrift))
    def fromWritable(x: BytesWritable): Fact = serialiser.fromBytes1(() => empty, x.getBytes)
    val mf: Manifest[SeqType] = implicitly
  }

  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  def parseErrorSeqSchema: SeqSchema[ParseError] = new SeqSchema[ParseError] {
    type SeqType = BytesWritable
    def empty = new ThriftParseError
    val serialiser = ThriftSerialiser()
    def toWritable(x: ParseError) = new BytesWritable(serialiser.toBytes(x.toThrift))
    def fromWritable(x: BytesWritable): ParseError =
      ParseError.fromThrift(serialiser.fromBytes1(() => empty, x.getBytes))

    val mf: Manifest[SeqType] = implicitly
  }

  /* WARNING THIS IS NOT SAFE TO EXPOSE, DANGER LURKS, SEE ThriftFactSeqSchema */
  private def mkThriftSchema[A](empty: A)(implicit ev: A <:< org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]) = new SeqSchema[A] {
    type SeqType = BytesWritable
    val serialiser = ThriftSerialiser()

    def toWritable(x: A) = new BytesWritable(serialiser.toBytes(x))
    def fromWritable(x: BytesWritable): A = serialiser.fromBytes(empty, x.getBytes)
    val mf: Manifest[SeqType] = implicitly
  }
}
