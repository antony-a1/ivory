package com.ambiata.ivory.snapshot

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import org.joda.time.{LocalDate, LocalDateTime}
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.mundane.time.DateTimex

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage._
import com.ambiata.ivory.validate.Validate
import com.ambiata.ivory.alien.hdfs._

case class HdfsSnapshot(repoPath: Path, store: String, dictName: String, entities: Option[Path], snapshot: LocalDate, errorPath: Path, storer: IvoryScoobiStorer[Fact, DList[_]]) {
  import IvoryStorage._

  type Priority = Int

  def withStorer(newStorer: IvoryScoobiStorer[Fact, DList[_]]): HdfsSnapshot =
    copy(storer = newStorer)

  def run: ScoobiAction[Unit] = for {
    r  <- ScoobiAction.value(Repository.fromHdfsPath(repoPath))
    d  <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dictName))
    s  <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    es <- ScoobiAction.fromHdfs(entities.traverseU(e => Hdfs.readWith(e, is =>  Streams.read(is)).map(_.lines.toSet)))
    _  <- scoobiJob(r, d, s, es)
  } yield ()

  def scoobiJob(repo: HdfsRepository, dict: Dictionary, store: FeatureStore, entities: Option[Set[String]]): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      // disable combiners as it's just overhead. The data is partitioned by date, so each mapper will have
      // only one date in it
      sc.disableCombiners
      factsFromIvoryStore(repo, store).map(input => {
        input.map(_.flatMap({ case (p, fs, f) =>
          Validate.validateFact(f, dict).map(_ => (p, fs, f)).disjunction
        }))

        val errors: DList[String] = input.collect {
          case -\/(e) => e
        }

        val facts: DList[(Priority, FactSetName, Fact)] = input.collect {
          case \/-((p, fs, f)) if f.date.isBefore(snapshot) && entities.map(_.contains(f.entity)).getOrElse(true) => (p, fs, f)
        }

        /*
         * 1. group by entity and feature id
         * 2. take the minimum fact in the group using fact time then priority to determine order
         */
        implicit val revDateOrder: Order[LocalDateTime] = DateTimex.LocalDateTimeHasOrder.reverseOrder
        val ord: Order[(Priority, FactSetName, Fact)] = Order.orderBy { case (p, _, f) => (f.time, p) }
        val latest: DList[Fact] = facts.groupBy { case (p, fs, f) => (f.entity, f.featureId.toString) }
                                       .reduceValues(Reduction.minimum(ord))
                                       .collect { case (_, (_, _, f)) if !f.isTombstone => f }

        persist(errors.toTextFile(errorPath.toString), storer.storeScoobi(latest))
        ()
      })
    }).flatten
}

object Snapshot {
  import DelimitedFactTextStorage._

  def onHdfs(repoPath: Path, store: String, dictName: String, entities: Option[Path], snapshot: LocalDate, output: Path, errorPath: Path): ScoobiAction[Unit] =
    HdfsSnapshot(repoPath, store, dictName, entities, snapshot, errorPath, DelimitedFactTextStorer(output)).run
}
