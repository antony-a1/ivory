package com.ambiata.ivory.extract

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import org.joda.time.LocalDate
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse._
import com.ambiata.mundane.time.DateTimex

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.SeqSchemas._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.validate.Validate
import com.ambiata.ivory.alien.hdfs._

case class HdfsSnapshot(repoPath: Path, store: String, dictName: String, entities: Option[Path], snapshot: LocalDate, outputPath: Path, errorPath: Path, incremental: Option[String]) {
  import IvoryStorage._

  // FIX this is inconsistent, sometimes a short, sometimes an int
  type Priority = Short

  val SnapshotName: String = "ivory-incremental-snapshot"
  lazy val snapshotDate = Date.fromLocalDate(snapshot)

  lazy val factsOutputPath = new Path(outputPath, "thrift")

  def run: ScoobiAction[Unit] = for {
    r  <- ScoobiAction.value(Repository.fromHdfsPath(repoPath))
    d  <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dictName))
    s  <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    es <- ScoobiAction.fromHdfs(entities.traverseU(e => Hdfs.readLines(e)))
    in <- incremental.traverseU(path => for {
      sm <- ScoobiAction.fromHdfs(SnapshotMeta.fromHdfs(new Path(path, ".snapmeta")))
      _   = println(s"Previous store was '${sm.store}'")
      _   = println(s"Previous date was '${sm.date.string("-")}'")
      s  <- ScoobiAction.fromHdfs(storeFromIvory(r, sm.store))
    } yield (path, s, sm))
    _  <- scoobiJob(r, d, s, es.map(_.toSet), in)
    _  <- ScoobiAction.fromHdfs(DictionaryTextStorage.DictionaryTextStorer(new Path(outputPath, "dictionary")).store(d))
    _  <- ScoobiAction.fromHdfs(SnapshotMeta(snapshotDate, store).toHdfs(new Path(factsOutputPath, ".snapmeta")))
  } yield ()

  def scoobiJob(repo: HdfsRepository, dict: Dictionary, store: FeatureStore, entities: Option[Set[String]], incremental: Option[(String, FeatureStore, SnapshotMeta)]): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      // disable combiners as it's just overhead. The data is partitioned by date, so each mapper will have
      // only one date in it
      sc.disableCombiners

      lazy val factsetMap = {
        val exclude = incremental.toList.flatMap(_._2.factSets).map(_.name).toSet
        val base = store.factSets.filter(fs => !exclude.contains(fs.name)).map(fs => (fs.priority.toShort, fs.name)).toMap
        base + (Short.MaxValue -> SnapshotName)
      }

      readFacts(repo, store, incremental).map(input => {

        val facts: DList[(Priority, Fact)] = input.map({
          case -\/(e) => sys.error("A critical error has occured, where we could not determine priority and namespace from partitioning: " + e)
          case \/-(v) => v
        }).collect({
          case (p, _, f) if f.date.isBefore(snapshotDate) && entities.map(_.contains(f.entity)).getOrElse(true) => (p.toShort, f)
        })

        /*
         * 1. group by entity and feature id
         * 2. take the minimum fact in the group using fact time then priority to determine order
         */
        val ord: Order[(Priority, Fact)] = Order.orderBy { case (p, f) => (-f.datetime.long, p) }
        val latest: DList[(Priority, Fact)] = facts.groupBy { case (p, f) => (f.entity, f.featureId.toString) }
                                                   .reduceValues(Reduction.minimum(ord))
                                                   .collect { case (_, (p, f)) if !f.isTombstone => (p, f) }

        val validated: DList[Fact] = latest.map({ case (p, f) =>
          Validate.validateFact(f, dict).disjunction.leftMap(e => e + " - Factset " + factsetMap.get(p).getOrElse("Unknown, priority " + p))
        }).map({
          case -\/(e) => sys.error("A critical error has occurred, a value in ivory no longer matches the dictionary: " + e)
          case \/-(v) => v
        })

        persist(validated.valueToSequenceFile(factsOutputPath.toString, overwrite = true).compressWith(new SnappyCodec))

        ()
      })
    }).flatten

  def readFacts(repo: HdfsRepository, store: FeatureStore, incremental: Option[(String, FeatureStore, SnapshotMeta)]): ScoobiAction[DList[ParseError \/ (Int, FactSetName, Fact)]] = {
    incremental match {
      case None             => factsFromIvoryStore(repo, store)
      case Some((p, s, sm)) => for {
        o <- factsFromIvoryStoreAfter(repo, s, sm.date) // only read facts from already processed factsets in the future
        sd = store --- s
        _  = println(s"Fully reading factsets '${sd.factSets}'")
        n <- factsFromIvoryStore(repo, sd) // fully read factsets which haven't been seen
      } yield o ++ n ++ valueFromSequenceFile[Fact](p).map(fact => (Short.MaxValue.toInt, SnapshotName, fact).right[ParseError])
    }
  }
}


object HdfsSnapshot {
  def takeSnapshot(repo: Path, output: Path, errors: Path, date: LocalDate, incremental: Option[String]): ScoobiAction[(String, String)] =
    fatrepo.ExtractLatestWorkflow.onHdfs(repo, extractLatest(output, errors, incremental), date)

  def extractLatest(outputPath: Path, errorPath: Path, incremental: Option[String])(repo: HdfsRepository, store: String, dictName: String, date: LocalDate): ScoobiAction[Unit] = for {
    d  <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dictName))
    _  <- HdfsSnapshot(repo.path, store, dictName, None, date, outputPath, errorPath, incremental).run
  } yield ()
}

case class SnapshotMeta(date: Date, store: String) {

  def toHdfs(path: Path): Hdfs[Unit] =
    Hdfs.writeWith(path, os => Streams.write(os, stringLines))

  lazy val stringLines: String =
    date.string("-") + "\n" + store
}

object SnapshotMeta {

  def fromHdfs(path: Path): Hdfs[SnapshotMeta] = for {
    raw <- Hdfs.readWith(path, is => Streams.read(is))
    sm  <- Hdfs.fromValidation(parser.run(raw.lines.toList))
  } yield sm

  def parser: ListParser[SnapshotMeta] = {
    import ListParser._
    for {
      d <- localDate
      s <- string.nonempty
    } yield SnapshotMeta(Date.fromLocalDate(d), s)
  }
}
