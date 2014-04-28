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
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.thrift._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.validate.Validate
import com.ambiata.ivory.alien.hdfs._

case class HdfsSnapshot(repoPath: Path, store: String, dictName: String, entities: Option[Path], snapshot: LocalDate, outputPath: Path, errorPath: Path, storer: IvoryScoobiStorer[Fact, DList[_]], inncremental: Option[(String, String)]) {
  import IvoryStorage._

  type Priority = Short

  def withStorer(newStorer: IvoryScoobiStorer[Fact, DList[_]]): HdfsSnapshot =
    copy(storer = newStorer)

  def run: ScoobiAction[Unit] = for {
    r  <- ScoobiAction.value(Repository.fromHdfsPath(repoPath))
    d  <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dictName))
    s  <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    es <- ScoobiAction.fromHdfs(entities.traverseU(e => Hdfs.readWith(e, is =>  Streams.read(is)).map(_.lines.toSet)))
    _  <- scoobiJob(r, d, s, es)
    _  <- storer.storeMeta
  } yield ()

  def scoobiJob(repo: HdfsRepository, dict: Dictionary, store: FeatureStore, entities: Option[Set[String]]): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      // disable combiners as it's just overhead. The data is partitioned by date, so each mapper will have
      // only one date in it
      sc.disableCombiners

      lazy val factsetMap = store.factSets.map(fs => (fs.priority.toShort, fs.name)).toMap

      factsFromIvoryStore(repo, store).map(input => {
        val errors: DList[String] = input.collect {
          case -\/(e) => e
        }

        val facts: DList[(Priority, Fact)] = input.collect {
          case \/-((p, _, f)) if f.date.isBefore(snapshot) && entities.map(_.contains(f.entity)).getOrElse(true) => (p.toShort, f)
        }

        /*
         * 1. group by entity and feature id
         * 2. take the minimum fact in the group using fact time then priority to determine order
         */
        implicit val revDateOrder: Order[LocalDateTime] = DateTimex.LocalDateTimeHasOrder.reverseOrder
        val ord: Order[(Priority, Fact)] = Order.orderBy { case (p, f) => (f.time, p) }
        val latest: DList[(Priority, Fact)] = facts.groupBy { case (p, f) => (f.entity, f.featureId.toString) }
                                                   .reduceValues(Reduction.minimum(ord))
                                                   .collect { case (_, (p, f)) if !f.isTombstone => (p, f) }

        val validated: DList[String \/ Fact] = latest.map({ case (p, f) =>
          Validate.validateFact(f, dict).disjunction.leftMap(e => e + " - Factset " + factsetMap.get(p).getOrElse("Unknown, priority " + p))
        })

        val valErrors = validated.collect {
          case -\/(e) => e
        }

        val good = validated.collect {
          case \/-(f) => f
        }

        // FIX ME when we are using a single view of "Fact" everywhere.
        implicit val ThriftWireFormat = WireFormats.mkThriftFmt(new ThriftFact)
        implicit val ThriftSchema = SeqSchemas.mkThriftSchema(new ThriftFact)
        import PartitionFactThriftStorageV1._
        val thrift = good.map(_.asThrift).valueToSequenceFile(new Path(outputPath, "thrift").toString)
        persist(errors.toTextFile((new Path(errorPath, "parse")).toString),
                valErrors.toTextFile((new Path(errorPath, "validation")).toString),
                thrift,
                storer.storeScoobi(good))
        ()
      })
    }).flatten
}
