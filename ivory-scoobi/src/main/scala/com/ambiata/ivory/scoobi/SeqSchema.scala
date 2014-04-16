package com.ambiata.ivory.scoobi

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.BytesWritable

import com.ambiata.ivory.thrift.ThriftSerialiser

object SeqSchemas {

  def mkThriftSchema[A](empty: A)(implicit ev: A <:< org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]) = new SeqSchema[A] {
    type SeqType = BytesWritable
    val serialiser = ThriftSerialiser()
    def toWritable(x: A) = new BytesWritable(serialiser.toBytes(x))
    def fromWritable(x: BytesWritable): A = serialiser.fromBytes(empty, x.getBytes)
    val mf: Manifest[SeqType] = implicitly
  }
}
