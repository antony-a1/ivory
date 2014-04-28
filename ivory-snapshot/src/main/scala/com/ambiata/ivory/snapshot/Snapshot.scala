package com.ambiata.ivory.snapshot

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import org.joda.time.{LocalDate, LocalDateTime}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.io._
import com.ambiata.mundane.time.DateTimex

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.SeqSchemas._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.validate.Validate
import com.ambiata.ivory.alien.hdfs._

case class HdfsSnapshot(repoPath: Path, store: String, dictName: String, entities: Option[Path], snapshot: LocalDate, outputPath: Path, errorPath: Path, storer: IvoryScoobiStorer[Fact, DList[_]], incremental: Option[(String, String)]) {
  import IvoryStorage._

  // FIX this is inconsistent, sometimes a short, sometimes an int
  type Priority = Short

  val SnapshotName: String = "ivory-incremental-snapshot"
  lazy val snapshotDate = Date.fromLocalDate(snapshot)

  def withStorer(newStorer: IvoryScoobiStorer[Fact, DList[_]]): HdfsSnapshot =
    copy(storer = newStorer)

  def run: ScoobiAction[Unit] = for {
    r  <- ScoobiAction.value(Repository.fromHdfsPath(repoPath))
    d  <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dictName))
    s  <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    es <- ScoobiAction.fromHdfs(entities.traverseU(e => Hdfs.readWith(e, is =>  Streams.read(is)).map(_.lines.toSet)))
    in <- incremental.traverseU({ case (path, store) => for {
      s  <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    } yield (path, s)  })
    _  <- scoobiJob(r, d, s, es, in)
    _  <- storer.storeMeta
  } yield ()

  def scoobiJob(repo: HdfsRepository, dict: Dictionary, store: FeatureStore, entities: Option[Set[String]], incremental: Option[(String, FeatureStore)]): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      // disable combiners as it's just overhead. The data is partitioned by date, so each mapper will have
      // only one date in it
      sc.disableCombiners

      lazy val factsetMap = {
        val exclude = incremental.toList.flatMap(_._2.factSets).map(_.name).toSet
        val base = store.factSets.filter(fs => !exclude.contains(fs.name)).map(fs => (fs.priority.toShort, fs.name)).toMap
        base + (Short.MaxValue -> SnapshotName)

      }

      val additional: DList[String \/ (Int, String, Fact)] = incremental match {
        case None =>
          DList[String \/ (Int, String, Fact)]()
        case Some((p, _)) =>
          implicit val fmt = mkThriftFmt(new ThriftFact)
          implicit val sch = mkThriftSchema(new ThriftFact)
          PartitionFactThriftStorageV2.loadScoobiWith[(Int, String, Fact)](p, (_, fact) => (Short.MaxValue.toInt, SnapshotName, fact).right[String])
      }

      factsFromIvoryStore(repo, store).map(base => {
        val input = base ++ additional

        val errors: DList[String] = input.collect {
          case -\/(e) => e
        }

        val facts: DList[(Priority, Fact)] = input.collect {
          case \/-((p, _, f)) if f.date.isBefore(snapshotDate) && entities.map(_.contains(f.entity)).getOrElse(true) => (p.toShort, f)
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
        import PartitionFactThriftStorageV2._

        val thrift = PartitionedFactThriftStorer(new Path(outputPath, "thrift").toString, Some(new SnappyCodec)).storeScoobi(good)

        persist(errors.toTextFile((new Path(errorPath, "parse")).toString),
                valErrors.toTextFile((new Path(errorPath, "validation")).toString),
                thrift,
                storer.storeScoobi(good))
        ()
      })
    }).flatten
}
