package com.ambiata.ivory.core.thrift

import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.{TBase, TFieldIdEnum, TSerializer, TDeserializer}

case class ThriftSerialiser() {
  val serialiser = new TSerializer(new TCompactProtocol.Factory)
  val deserialiser = new TDeserializer(new TCompactProtocol.Factory)

  def toBytes[A](a: A)(implicit ev: A <:< TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]): Array[Byte] =
    serialiser.serialize(ev(a))

  def fromBytes[A](empty: A, bytes: Array[Byte])(implicit ev: A <:< TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]): A = {
    val e = ev(empty).deepCopy
    e.clear
    deserialiser.deserialize(e, bytes)
    e.asInstanceOf[A]
  }

  def fromBytes1[A](empty: () => A, bytes: Array[Byte])(implicit ev: A <:< TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]): A = {
    val e = empty()
    deserialiser.deserialize(e, bytes)
    e.asInstanceOf[A]
  }

}
