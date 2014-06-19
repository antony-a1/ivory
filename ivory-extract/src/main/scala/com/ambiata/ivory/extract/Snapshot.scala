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

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.SeqSchemas._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.validate.Validate
import com.ambiata.ivory.alien.hdfs._

case class HdfsSnapshot(repoPath: Path, store: String, dictName: String, entities: Option[Path], snapshot: Date, outputPath: Path, errorPath: Path, incremental: Option[(Path, SnapshotMeta)], codec: Option[CompressionCodec], fast: Boolean) {
  import IvoryStorage._

  def run: ScoobiAction[Unit] = for {
    r    <- ScoobiAction.scoobiConfiguration.map(sc => Repository.fromHdfsPath(repoPath.toString.toFilePath, sc))
    d    <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dictName))
    s    <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    es   <- ScoobiAction.fromHdfs(entities.traverseU(e => Hdfs.readLines(e)))
    in   <- incremental.traverseU({ case (path, sm) => for {
              _ <- ScoobiAction.value({
                     println(s"Previous store was '${sm.store}'")
                     println(s"Previous date was '${sm.date.string("-")}'")
                   })
              s <- ScoobiAction.fromHdfs(storeFromIvory(r, sm.store))
            } yield (path, s, sm) })
    _    <- if(fast) scoobiLight(r, s, in) else scoobiJob(r, d, s, es.map(_.toSet), in, codec)
    _    <- ScoobiAction.fromHdfs(DictionaryTextStorage.DictionaryTextStorer(new Path(outputPath, ".dictionary")).store(d))
    _    <- ScoobiAction.fromHdfs(SnapshotMeta(snapshot, store).toHdfs(new Path(outputPath, SnapshotMeta.fname)))
  } yield ()

  def scoobiLight(repo: HdfsRepository, store: FeatureStore, incremental: Option[(Path, FeatureStore, SnapshotMeta)]): ScoobiAction[Unit] =
    ScoobiAction.fromHdfs(for {
      conf  <- Hdfs.configuration
      globs <- HdfsSnapshot.storePaths(repo, store, snapshot, incremental)
      paths  = globs.flatMap(_.partitions.map(p => new Path(p.path))) ++ incremental.map(_._1).toList
      size  <- paths.traverse(Hdfs.size).map(_.sum)
      _        = println(s"Total input size: ${size}")
      reducers = size / 1024 / 1024 / 768 + 1 // one reducer per 768MB of input
      _        = println(s"Number of reducers: ${reducers}")
      _     <- Hdfs.safe(SnapshotJob.run(conf, reducers.toInt, snapshot, globs, outputPath, incremental.map(_._1)))
    } yield ())

  def scoobiJob(repo: HdfsRepository, dict: Dictionary, store: FeatureStore, entities: Option[Set[String]], incremental: Option[(Path, FeatureStore, SnapshotMeta)], codec: Option[CompressionCodec]): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      // disable combiners as it's just overhead. The data is partitioned by date, so each mapper will have
      // only one date in it
      sc.disableCombiners

      lazy val factsetMap: Map[Priority, Factset] = {
        val exclude = incremental.toList.flatMap(_._2.factsets).map(_.set.name).toSet
        val base = store.factsets.filter(fs => !exclude.contains(fs.set.name)).map(fs => (fs.priority, fs.set)).toMap
        base + (Priority.Max -> Factset(HdfsSnapshot.SnapshotName))
      }

      HdfsSnapshot.readFacts(repo, store, snapshot, incremental).map(input => {

        val facts: DList[(Priority, Fact)] = input.map({
          case -\/(e) => sys.error("A critical error has occured, where we could not determine priority and namespace from partitioning: " + e)
          case \/-(v) => v
        }).collect({
          case (p, _, f) if f.date.isBefore(snapshot) && entities.map(_.contains(f.entity)).getOrElse(true) => (p, f)
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

        val toPersist = validated.valueToSequenceFile(outputPath.toString, overwrite = true)
        persist(codec.map(toPersist.compressWith(_)).getOrElse(toPersist))

        ()
      })
    }).flatten
}


object HdfsSnapshot {
  val SnapshotName: String = "ivory-incremental-snapshot"

  def takeSlowSnapshot(repoPath: Path, errors: Path, date: Date, incremental: Boolean, codec: Option[CompressionCodec]): ScoobiAction[(String, String, Path)] =
    fatrepo.ExtractLatestWorkflow.onHdfs(repoPath, extractLatest(errors, codec, false), date, incremental)

  def takeSnapshot(repoPath: Path, errors: Path, date: Date, incremental: Boolean, codec: Option[CompressionCodec]): ScoobiAction[(String, String, Path)] =
    fatrepo.ExtractLatestWorkflow.onHdfs(repoPath, extractLatest(errors, codec, true), date, incremental)

  def extractLatest(errorPath: Path, codec: Option[CompressionCodec], fast: Boolean)(repo: HdfsRepository, store: String, dictName: String, date: Date, outputPath: Path, incremental: Option[(Path, SnapshotMeta)]): ScoobiAction[Unit] =
    HdfsSnapshot(repo.root.toHdfs, store, dictName, None, date, outputPath, errorPath, incremental, codec, fast).run

  def readFacts(repo: HdfsRepository, store: FeatureStore, latestDate: Date, incremental: Option[(Path, FeatureStore, SnapshotMeta)]): ScoobiAction[DList[ParseError \/ (Priority, Factset, Fact)]] = {
    import IvoryStorage._
    incremental match {
      case None =>
        factsFromIvoryStoreTo(repo, store, latestDate)
      case Some((p, s, sm)) => for {
        o <- factsFromIvoryStoreBetween(repo, s, sm.date, latestDate) // read facts from already processed store from the last snapshot date to the latest date
        sd = store --- s
        _  = println(s"Reading factsets '${sd.factsets}' up to '${latestDate}'")
        n <- factsFromIvoryStoreTo(repo, sd, latestDate) // read factsets which haven't been seen up until the 'latest' date
      } yield o ++ n ++ valueFromSequenceFile[Fact](p.toString).map(fact => (Priority.Max, Factset(SnapshotName), fact).right[ParseError])
    }
  }

  def storePaths(repo: HdfsRepository, store: FeatureStore, latestDate: Date, incremental: Option[(Path, FeatureStore, SnapshotMeta)]): Hdfs[List[FactsetGlob]] = Hdfs.fromResultTIO({
    incremental match {
      case None =>
        StoreGlob.before(repo, store, latestDate)
      case Some((p, s, sm)) => for {
        o <- StoreGlob.between(repo, s, sm.date, latestDate) // read facts from already processed store from the last snapshot date to the latest date
        sd = store --- s
        _  = println(s"Reading factsets '${sd.factsets}' up to '${latestDate}'")
        n <- StoreGlob.before(repo, sd, latestDate) // read factsets which haven't been seen up until the 'latest' date
      } yield FactsetGlob.groupByVersion(o ++ n)
    }
  })
}
