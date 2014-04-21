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

object PartitionFactThriftStorageV1 {

  type FactsetName = String
  type Priority = Int

  def loadScoobiWith[A : WireFormat](path: String, f: (FactsetName, Fact) => String \/ A)(implicit sc: ScoobiConfiguration): DList[String \/ A] = {
    implicit val fmt = mkThriftFmt(new ThriftFact)
    implicit val sch = mkThriftSchema(new ThriftFact)
     valueFromSequenceFileWithPath[ThriftFact](path).map { case (partition, tfact) =>
      for {
        p              <- Partition.parseWith(new java.net.URI(partition).getPath).disjunction
        (fs, ns, date)  = p
        fact           <- tfact.asFact(ns, date)
        a              <- f(fs, fact)
      } yield a
    }
  }
  
  case class PartitionedFactThriftLoader(path: String) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[String \/ Fact] =
      loadScoobiWith(path+"/*/*/*/*/*", (_, fact) => fact.right)
  }

  case class PartitionedMultiFactsetThriftLoader(base: String, factsets: List[FactSet]) extends IvoryScoobiLoader[(Priority, FactsetName, Fact)] {
    lazy val factsetMap: Map[String, Int] = factsets.map(fs => (fs.name, fs.priority)).toMap

    def loadScoobi(implicit sc: ScoobiConfiguration): DList[String \/ (Int, String, Fact)] = {
      loadScoobiWith(base + "/{" + factsets.map(_.name).mkString(",") + "}/*/*/*/*/*", (fs, fact) =>
        factsetMap.get(fs).map(pri => (pri, fs, fact).right).getOrElse(s"Factset '${fs}' not found in expected list '${factsets}'".left))
    }
  }

  case class PartitionedFactThriftStorer(base: String, codec: Option[CompressionCodec] = None) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
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

object PartitionFactThriftStorageV2 {
  
  type FactsetName = String

  def loadScoobiWith[A : WireFormat](path: String, f: (FactsetName, Fact) => String \/ A): DList[String \/ A] = {
    implicit val fmt = mkThriftFmt(new ThriftFact)
    implicit val sch = mkThriftSchema(new ThriftFact)
     valueFromSequenceFileWithPath[ThriftFact](path).map { case (partition, tfact) =>
      for {
        p              <- Partition.parseWith(new java.net.URI(partition).getPath).disjunction
        (fs, ns, date)  = p
        fact           <- tfact.asFact(ns, date)
        a              <- f(fs, fact)
      } yield a
    }
  }
  
  case class PartitionedFactThriftLoader(path: String) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[String \/ Fact] =
      loadScoobiWith(path+"/*/*/*/*/*", (_, fact) => fact.right)
  }

  case class PartitionedMultiFactsetThriftLoader(base: String, factsets: List[FactSet]) extends IvoryScoobiLoader[(Int, String, Fact)] {
    lazy val factsetMap: Map[String, Int] = factsets.map(fs => (fs.name, fs.priority)).toMap

    def loadScoobi(implicit sc: ScoobiConfiguration): DList[String \/ (Int, String, Fact)] = {
      loadScoobiWith(base + "/{" + factsets.map(_.name).mkString(",") + "}/*/*/*/*/*", (fs, fact) =>
        factsetMap.get(fs).map(pri => (pri, fs, fact).right).getOrElse(s"Factset '${fs}' not found in expected list '${factsets}'".left))
    }
  }
  
  case class PartitionedFactThriftStorer(base: String, codec: Option[CompressionCodec] = None) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
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
