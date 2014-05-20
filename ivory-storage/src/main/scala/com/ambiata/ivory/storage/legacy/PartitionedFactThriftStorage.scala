package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress.{SnappyCodec, CompressionCodec}
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.scoobi._
import WireFormats._
import FactFormats._
import SeqSchemas._

import scala.collection.JavaConverters._
import java.net.URI

object PartitionFactThriftStorageV1 {

  def loadScoobiWith[A : WireFormat](paths: List[String], f: (Factset, Fact) => ParseError \/ A, from: Option[Date] = None, to: Option[Date] = None)(implicit sc: ScoobiConfiguration): DList[ParseError \/ A] = {
    val filtered = PartitionExpantion.filterGlob(paths, from, to).toSeq
    if(!filtered.isEmpty)
      valueFromSequenceFileWithPaths[ThriftFact](filtered.toSeq).map({ case (partition, tfact) =>
        for {
          p    <- Partition.parseWith(new URI(partition).toString).leftMap(ParseError.withLine(new URI(partition).toString)).disjunction
          fact  = FatThriftFact(p.namespace, p.date, tfact)
          a    <- f(p.factset, fact)
        } yield a
      })
    else
      DList[ParseError \/ A]()
  }

  case class PartitionedFactThriftLoader(paths: List[String], from: Option[Date] = None, to: Option[Date] = None) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
      loadScoobiWith(paths.map(_+"/*/*/*/*"), (_, fact) => fact.right, from, to)
  }

  case class PartitionedMultiFactsetThriftLoader(base: String, factsets: List[PrioritizedFactset], from: Option[Date] = None, to: Option[Date] = None) extends IvoryScoobiLoader[(Priority, Factset, Fact)] {
    lazy val factsetMap: Map[Factset, Priority] = factsets.map(fs => (fs.set, fs.priority)).toMap

    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ (Priority, Factset, Fact)] = {
      loadScoobiWith(factsets.map(base + "/" + _.set.name + "/*/*/*/*"), (fs, fact) =>
        factsetMap.get(fs).map(pri => (pri, fs, fact).right).getOrElse(ParseError(fs.name, s"Factset '${fs}' not found in expected list '${factsets}'").left), from, to)
    }
  }

  case class PartitionedFactThriftStorer(base: String) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] = {
      val partitioned = dlist.by(f => Partition.path(f.namespace, f.date))
                             .mapValues((f: Fact) => f.toThrift)
                             .valueToPartitionedSequenceFile[PartitionKey, ThriftFact](base, identity, overwrite = true)
      partitioned.compressWith(new SnappyCodec)
    }
  }
}

object PartitionFactThriftStorageV2 {

  def loadScoobiWith[A : WireFormat](paths: List[String], f: (Factset, Fact) => ParseError \/ A, from: Option[Date] = None, to: Option[Date] = None)(implicit sc: ScoobiConfiguration): DList[ParseError \/ A] = {
    val filtered = PartitionExpantion.filterGlob(paths, from, to).toSeq
    if(!filtered.isEmpty)
      valueFromSequenceFileWithPaths[ThriftFact](filtered.toSeq).map({ case (partition, tfact) =>
        for {
          p    <- Partition.parseWith(new URI(partition).toString).leftMap(ParseError.withLine(new URI(partition).toString)).disjunction
          fact  = FatThriftFact(p.namespace, p.date, tfact)
          a    <- f(p.factset, fact)
        } yield a
      })
    else
      DList[ParseError \/ A]()
  }

  case class PartitionedFactThriftLoader(paths: List[String], from: Option[Date] = None, to: Option[Date] = None) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
      loadScoobiWith(paths.map(_+"/*/*/*/*"), (_, fact) => fact.right, from, to)
  }

  case class PartitionedMultiFactsetThriftLoader(base: String, factsets: List[PrioritizedFactset], from: Option[Date] = None, to: Option[Date] = None) extends IvoryScoobiLoader[(Priority, Factset, Fact)] {
    lazy val factsetMap: Map[Factset, Priority] = factsets.map(fs => (fs.set, fs.priority)).toMap

    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ (Priority, Factset, Fact)] = {
      loadScoobiWith(factsets.map(base + "/" + _.set.name + "/*/*/*/*"), (fs, fact) =>
        factsetMap.get(fs).map(pri => (pri, fs, fact).right).getOrElse(ParseError(fs.name, s"Factset '${fs}' not found in expected list '${factsets}'").left), from, to)
    }
  }

  case class PartitionedFactThriftStorer(base: String) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] = {
      val partitioned = dlist.by(f => Partition.path(f.namespace, f.date))
                             .mapValues((f: Fact) => f.toThrift)
                             .valueToPartitionedSequenceFile[PartitionKey, ThriftFact](base, identity, overwrite = true)
      partitioned.compressWith(new SnappyCodec)
    }
  }
}

object PartitionExpantion {
  def filterGlob(paths: List[String], from: Option[Date], to: Option[Date])(implicit sc: ScoobiConfiguration): List[String] =
    (from, to) match {
      case (None, None)         => paths.map(_+"/*")
      case (None, Some(td))     => filterGlobWith(paths, p => Partitions.pathsBeforeOrEqual(p, td))
      case (Some(fd), None)     => filterGlobWith(paths, p => Partitions.pathsAfterOrEqual(p, fd))
      case (Some(fd), Some(td)) => filterGlobWith(paths, p => Partitions.pathsBetween(p, fd, td))
    }

  def filterGlobWith(paths: List[String], f: List[Partition] => List[Partition])(implicit sc: ScoobiConfiguration): List[String] = (for {
    expanded   <- paths.traverse(p => Hdfs.globPaths(new Path(p))).map(_.flatten)
    partitions <- expanded.traverse(p => Hdfs.fromValidation(Partition.parseWith(p.toString)))
  } yield f(partitions)).run(sc).run.unsafePerformIO() match {
    case Error(e) => sys.error(s"Could not access hdfs - ${e}")
    case Ok(glob) => glob.map(_.path)
  }
}
