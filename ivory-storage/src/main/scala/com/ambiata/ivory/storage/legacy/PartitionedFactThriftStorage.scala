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

  type FactsetName = String
  type Priority = Int

  def loadScoobiWith[A : WireFormat](path: String, f: (FactsetName, Fact) => ParseError \/ A, after: Option[Date] = None)(implicit sc: ScoobiConfiguration): DList[ParseError \/ A] = {
    expandGlob(path, after).map(glob => {
      valueFromSequenceFileWithPath[ThriftFact](glob).map { case (partition, tfact) =>
        for {
          p    <- Partition.parseWith(new URI(partition).toString).leftMap(ParseError.withLine(new URI(partition).toString)).disjunction
          fact  = FatThriftFact(p.namespace, p.date, tfact)
          a    <- f(p.factset, fact)
        } yield a
      }
    }).getOrElse(DList[ParseError \/ A]())
  }

  def expandGlob(path: String, after: Option[Date])(implicit sc: ScoobiConfiguration): Option[String] =
    after.map(date => (for {
      paths      <- Hdfs.globFiles(new Path(path))
      partitions <- paths.traverse(p => Hdfs.fromValidation(Partition.parseWith(p.toString)))
    } yield Partitions.globPathAfter(partitions, date)).run(sc).run.unsafePerformIO() match {
      case Error(e) => sys.error(s"Could not access hdfs - ${e}")
      case Ok(glob) => glob
    }).getOrElse(Some(path))

  case class PartitionedFactThriftLoader(path: String, after: Option[Date] = None) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
      loadScoobiWith(path+"/*/*/*/*/*", (_, fact) => fact.right, after)
  }

  case class PartitionedMultiFactsetThriftLoader(base: String, factsets: List[FactSet], after: Option[Date] = None) extends IvoryScoobiLoader[(Priority, FactsetName, Fact)] {
    lazy val factsetMap: Map[String, Int] = factsets.map(fs => (fs.name, fs.priority)).toMap

    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ (Int, String, Fact)] = {
      loadScoobiWith(base + "/{" + factsets.map(_.name).mkString(",") + "}/*/*/*/*/*", (fs, fact) =>
        factsetMap.get(fs).map(pri => (pri, fs, fact).right).getOrElse(ParseError(fs, s"Factset '${fs}' not found in expected list '${factsets}'").left), after)
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

  type FactsetName = String

  def loadScoobiWith[A : WireFormat](path: String, f: (FactsetName, Fact) => ParseError \/ A, after: Option[Date] = None)(implicit sc: ScoobiConfiguration): DList[ParseError \/ A] = {
    expandGlob(path, after).map(glob => {
      valueFromSequenceFileWithPath[ThriftFact](glob).map { case (partition, tfact) =>
        for {
          p    <- Partition.parseWith(new URI(partition).toString).leftMap(ParseError.withLine(new URI(partition).toString)).disjunction
          fact  = FatThriftFact(p.namespace, p.date, tfact)
          a    <- f(p.factset, fact)
        } yield a
      }
    }).getOrElse(DList[ParseError \/ A]())
  }

  def expandGlob(path: String, after: Option[Date])(implicit sc: ScoobiConfiguration): Option[String] =
    after.map(date => (for {
      paths      <- Hdfs.globFiles(new Path(path))
      partitions <- paths.traverse(p => Hdfs.fromValidation(Partition.parseWith(p.toString)))
    } yield Partitions.globPathAfter(partitions, date)).run(sc).run.unsafePerformIO() match {
      case Error(e) => sys.error(s"Could not access hdfs - ${e}")
      case Ok(glob) => glob
    }).getOrElse(Some(path))

  case class PartitionedFactThriftLoader(path: String, after: Option[Date] = None) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
      loadScoobiWith(path+"/*/*/*/*/*", (_, fact) => fact.right, after)
  }

  case class PartitionedMultiFactsetThriftLoader(base: String, factsets: List[FactSet], after: Option[Date] = None) extends IvoryScoobiLoader[(Int, String, Fact)] {
    lazy val factsetMap: Map[String, Int] = factsets.map(fs => (fs.name, fs.priority)).toMap

    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ (Int, String, Fact)] = {
      loadScoobiWith(base + "/{" + factsets.map(_.name).mkString(",") + "}/*/*/*/*/*", (fs, fact) =>
        factsetMap.get(fs).map(pri => (pri, fs, fact).right).getOrElse(ParseError(fs, s"Factset '${fs}' not found in expected list '${factsets}'").left), after)
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

