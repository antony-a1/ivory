package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress.CompressionCodec

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.scoobi._
import WireFormats._
import FactFormats._
import SeqSchemas._

import scala.collection.JavaConverters._

object PartitionFactThriftStorageV1 {

  type FactsetName = String
  type Priority = Int

  def loadScoobiWith[A : WireFormat](path: String, f: (FactsetName, Fact) => String \/ A)(implicit sc: ScoobiConfiguration): DList[String \/ A] = {
     valueFromSequenceFileWithPath[ThriftFact](path).map { case (partition, tfact) =>
      for {
        p          <- Partition.parseWith(new java.net.URI(partition).getPath).disjunction
        (fs, ns, d) = p
        fact        =  FatThriftFact(ns, d, tfact)
        a          <- f(fs, fact)
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
      val partitioned = dlist.by(f => Partition.path(f.namespace, f.date))
                             .mapValues((f: Fact) => f.toThrift)
                             .valueToPartitionedSequenceFile[PartitionKey, ThriftFact](base, identity, overwrite = true)
      codec.map(c => partitioned.compressWith(c)).getOrElse(partitioned)
    }
  }
}

object PartitionFactThriftStorageV2 {

  type FactsetName = String

  def loadScoobiWith[A : WireFormat](path: String, f: (FactsetName, Fact) => String \/ A): DList[String \/ A] = {
     valueFromSequenceFileWithPath[ThriftFact](path).map { case (partition, tfact) =>
      for {
        p          <- Partition.parseWith(new java.net.URI(partition).getPath).disjunction
        (fs, ns, d) = p
        fact        = FatThriftFact(ns, d, tfact)
        a          <- f(fs, fact)
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
      val partitioned = dlist.by(f => Partition.path(f.namespace, f.date))
                             .mapValues((f: Fact) => f.toThrift)
                             .valueToPartitionedSequenceFile[PartitionKey, ThriftFact](base, identity, overwrite = true)
      codec.map(c => partitioned.compressWith(c)).getOrElse(partitioned)
    }
  }
}
