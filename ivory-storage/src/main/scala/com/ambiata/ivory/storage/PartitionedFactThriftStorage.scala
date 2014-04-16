package com.ambiata.ivory.storage

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.joda.time.LocalDate
import org.apache.hadoop.io.compress.CompressionCodec

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.thrift._
import WireFormats._
import SeqSchemas._

import scala.collection.JavaConverters._

object PartitionFactThriftStorage {
  
  case class PartitionedFactThriftLoaderV1(path: String) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[String \/ Fact] = {
      implicit val fmt = mkThriftFmt(new ThriftFact)
      implicit val sch = mkThriftSchema(new ThriftFact)
       valueFromSequenceFileWithPath[ThriftFact](path+"/*/*/*/*/*").map { case (partition, tfact) =>
        for {
          p             <- Partition.parseWith(new java.net.URI(partition).getPath).disjunction
          (_, ns, date)  = p
          fact          <- tfact.asFact(ns, date)
        } yield fact
      }
    }
  }

  case class PartitionedFactThriftLoaderV2(path: String) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[String \/ Fact] = {
      implicit val fmt = mkThriftFmt(new ThriftFact)
      implicit val sch = mkThriftSchema(new ThriftFact)
       valueFromSequenceFileWithPath[ThriftFact](path+"/*/*/*/*/*").map { case (partition, tfact) =>
        for {
          p             <- Partition.parseWith(new java.net.URI(partition).getPath).disjunction
          (_, ns, date)  = p
          fact          <- tfact.asFact(ns, date)
        } yield fact
      }
    }
  }

  case class PartitionedFactThriftStorerV1(base: String, codec: Option[CompressionCodec] = None) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] = {
      implicit val fmt = mkThriftFmt(new ThriftFact)
      implicit val sch = mkThriftSchema(new ThriftFact)
      val partitioned = dlist.by(f => Partition.path(f.featureId.namespace, f.date))
                             .mapValues((f: Fact) => f.asThrift)
                             .valueToPartitionedSequenceFile[PartitionKey, ThriftFact](base, identity, overwrite = true)
      codec.map(c => partitioned.compressWith(c)).getOrElse(partitioned)
    }
  }

  case class PartitionedFactThriftStorerV2(base: String, codec: Option[CompressionCodec] = None) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] = {
      implicit val fmt = mkThriftFmt(new ThriftFact)
      implicit val sch = mkThriftSchema(new ThriftFact)
      val partitioned = dlist.by(f => Partition.path(f.featureId.namespace, f.date))
                             .mapValues((f: Fact) => f.asThrift)
                             .valueToPartitionedSequenceFile[PartitionKey, ThriftFact](base, identity, overwrite = true)
      codec.map(c => partitioned.compressWith(c)).getOrElse(partitioned)
    }
  }

  implicit class FactThriftSyntax(fact: Fact) {
    def asThrift: ThriftFact = {
      val tfact = new ThriftFact(fact.entity, fact.featureId.name, valueAsThrift(fact.value))
      if(fact.seconds > 0) tfact.setSeconds(fact.seconds) else tfact
    }

    def valueAsThrift(value: Value): ThriftFactValue = value match {
      case BooleanValue(b)  => ThriftFactValue.b(b)
      case IntValue(i)      => ThriftFactValue.i(i)
      case LongValue(l)     => ThriftFactValue.l(l)
      case DoubleValue(d)   => ThriftFactValue.d(d)
      case StringValue(s)   => ThriftFactValue.s(s)
      case TombstoneValue() => ThriftFactValue.t(new ThriftTombstone())
    }
  }

  implicit class ThriftFactSyntax(tfact: ThriftFact) {
    def asFact(namespace: String, date: LocalDate): String \/ Fact = {
      val value = tfact.getValue match {
        case tv if(tv.isSetS) => StringValue(tv.getS).right
        case tv if(tv.isSetB) => BooleanValue(tv.getB).right
        case tv if(tv.isSetI) => IntValue(tv.getI).right
        case tv if(tv.isSetL) => LongValue(tv.getL).right
        case tv if(tv.isSetD) => DoubleValue(tv.getD).right
        case tv if(tv.isSetT) => TombstoneValue().right
        case _                => s"Unrecognised value in thrift record '${tfact.toString}'!".left
      }
      value.map(v => Fact(tfact.getEntity, FeatureId(namespace, tfact.getAttribute), date, Option(tfact.getSeconds).getOrElse(0), v))
    }
  }
}
