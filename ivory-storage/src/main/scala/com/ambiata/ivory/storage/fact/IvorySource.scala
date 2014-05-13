package com.ambiata.ivory.storage.fact

import com.ambiata.mundane.control._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.scoobi._, FactFormats._, WireFormats._, SeqSchemas._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.parse._
import com.nicta.scoobi._, Scoobi._

import java.net.URI

import org.apache.hadoop.fs.Path
import org.joda.time.DateTimeZone

import scalaz.{DList => _, _}, Scalaz._
import scalaz.concurrent._
import scalaz.effect._
import scalaz.stream._

sealed trait IvorySource {
  def toDList: DList[ParseError \/ (Priority, Fact)]
//  def toProcess: Process[Task, (Priority, Fact)]
}

object IvorySource {
  def fromEavt(dictionary: Dictionary, namespace: String, path: String, zone: DateTimeZone, priority: Priority): IvorySource = new IvorySource {
    def toDList =
      fromTextFile(path).map(l => EavtParsers.fact(dictionary, namespace, zone).run(EavtParsers.splitLine(l)).leftMap(ParseError.withLine(l)).disjunction.map(priority -> _))
  }

  def fromRepository(repository: Repository, store: FeatureStore): ResultT[IO, IvorySource] = repository match {
    case HdfsRepository(root, conf, run) =>
      store.factsets.traverseU(factset =>
        Versions.read(repository, factset.set).map(version =>
          valueFromSequenceFileWithPaths[ThriftFact](Seq(s"${repository.factset(factset.set).path}/*/*/*/*")).map({
            case (partition, tfact) =>
              Partition.parseWith(new URI(partition).toString).leftMap(ParseError.withLine(new URI(partition).toString)).disjunction.flatMap(p =>
                (factset.priority, FatThriftFact(p.namespace, p.date, tfact)).right)
          }))).map(dlists =>
            new IvorySource {
              def toDList: DList[ParseError \/ (Priority, Fact)] =
                dlists.foldLeft(DList[ParseError \/ (Priority, Fact)]())(_ ++ _)
            })
    case LocalRepository(root) =>
      // FIX implement
      ResultT.fail("Local not currently supported.")
    case S3Repository(bucket, root, conf, client, tmp, run) =>
      // FIX implement
      ResultT.fail("S3 not currently supported.")
  }

  def fromThriftExtract(path: String): IvorySource =
    new IvorySource {
      def toDList: DList[ParseError \/ (Priority, Fact)] =
        valueFromSequenceFile[Fact](Seq(path)).map(f => (Priority.Max, f).right)
    }
}
