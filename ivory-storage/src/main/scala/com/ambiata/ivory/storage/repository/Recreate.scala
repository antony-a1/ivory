package com.ambiata.ivory
package storage
package repository

import com.ambiata.ivory.alien.hdfs.Hdfs
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage.legacy.{FlatFactThriftStorageV1, IvoryStorage}
import com.ambiata.mundane.io.FilePath
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodec
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.FactFormats._
import IvoryStorage._
import ScoobiAction.scoobiJob

import scalaz.{DList => _, _}, Scalaz._, \&/._
import RecreateAction._
import IvorySyntax._

/**
 * Recreate actions for recreating parts or all of a repository
 */
object Recreate {
  def all: RecreateAction[Unit] =
    metadata. log("****** Recreating metadata")   >>
    factsets. log("****** Recreating factsets")   >>
    snapshots.log(s"****** Recreating snapshots")

  def metadata: RecreateAction[Unit] =
    dictionaries.log(s"****** Recreating dictionaries") >>
    stores.      log(s"****** Recreating stores")

  def dictionaries: RecreateAction[Unit] =
    recreate("dictionaries", (_:Repository).dictionaries) { conf =>
      fromHdfs(copyDictionaries(conf.hdfsFrom, conf.hdfsTo, conf.dry))
    }

  def stores: RecreateAction[Unit] =
    recreate("stores", (_:Repository).stores) { conf =>
      fromHdfs(copyStores(conf.hdfsFrom, conf.hdfsTo, conf.clean, conf.dry))
    }

  def factsets: RecreateAction[Unit] =
    recreate("factsets", (_:Repository).factsets) { conf =>
      fromScoobi(copyFactsets(conf.hdfsFrom, conf.hdfsTo, conf.codec, conf.dry))
    }

  def snapshots: RecreateAction[Unit] =
    recreate("snapshots", (_:Repository).snapshots) { conf =>
      fromScoobi(copySnapshots(conf.hdfsFrom, conf.hdfsTo, conf.codec, conf.dry))
    }

  /**
   * recreate a given set of data and log before/after count and size
   */
  private def recreate[A, V](name: String, f: Repository => FilePath)(action: RecreateConfig => RecreateAction[A]): RecreateAction[Unit] =
    configuration.flatMap { conf =>
      logStat("Number of "+name, conf.from, StatAction.numberOf(f)) >>
      logStat("Size of "+name, conf.from, StatAction.showSizeOfInBytes(f)) >>
        action(conf) >>
        unless (conf.dry) {
          logStat("Number of "+name, conf.from, StatAction.numberOf(f)) >>
          logStat("Size of "+name, conf.from, StatAction.sizeOf(f))
        }
    }

  /**
   * DICTIONARIES
   */
  private def copyDictionaries(from: HdfsRepository, to: HdfsRepository, dry: Boolean): Hdfs[Unit] =
    Hdfs.mkdir(to.dictionaries.toHdfs).unless(dry) >>
    Hdfs.globPaths(from.dictionaries.toHdfs).flatMap(_.traverse(copyDictionary(from, to, dry))).void

  private def copyDictionary(from: HdfsRepository, to: HdfsRepository, dry: Boolean) = (path: Path) =>
    Hdfs.log(s"${from.dictionaryByName(path.getName)} -> ${to.dictionaryByName(path.getName)}") >>
    dictionaryPartsFromIvory(from, path.getName).map(dicts => dictionariesToIvory(to, dicts, path.getName)).unless(dry)

  /**
   * STORES
   */
  private def copyStores(from: HdfsRepository, to: HdfsRepository, clean: Boolean, dry: Boolean): Hdfs[Unit] =
    Hdfs.mkdir(to.stores.toHdfs).unless(dry) >>
    (nonEmptyFactsetsNames(from) |@| storesPaths(from)) { (names, stores) =>
      stores.traverse(copyStore(from, to, clean, dry, names))
    }

  private def copyStore(from: HdfsRepository, to: HdfsRepository, clean: Boolean, dry: Boolean, filtered: Set[String]) = (path: Path) =>
    for {
      _       <- Hdfs.log(s"${from.storeByName(path.getName)} -> ${to.storeByName(path.getName)}")
      store   <- storeFromIvory(from, path.getName)
      cleaned <- cleanupStore(path.getName, store, filtered, clean)
      _       <- storeToIvory(to, cleaned, path.getName).unless(dry)
    } yield ()

  private def cleanupStore(name: String, store: FeatureStore, setsToKeep: Set[String], clean: Boolean) = {
    val cleaned = if (clean) store.filter(setsToKeep) else store
    val removed = store.diff(cleaned).factsets.map(_.name)
    Hdfs.log(s"Removed factsets '${removed.mkString(",")}' from feature store '$name' as they are empty.").unless(removed.isEmpty) >>
    Hdfs.safe(cleaned)
  }

  private def storesPaths(from: Repository): Hdfs[List[Path]] =
    Hdfs.globFiles(from.stores.toHdfs)

  /**
   * FACTSETS
   */
  private def copyFactsets(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec], dry: Boolean): ScoobiAction[Unit] =
    ScoobiAction.fromHdfs(nonEmptyFactsetsNames(from)).flatMap(_.toList.traverse(copyFactset(from, to, codec, dry))).unless(dry)

  private def copyFactset(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec], dry: Boolean) = (name: String) =>
    factsFromIvoryFactset(from, Factset(name)).map(_ map throwAwayErrors("Could not load facts")).flatMap { facts =>
      scoobiJob(sc => facts.toIvoryFactset(to, Factset(name), codec)(sc).persist(sc)).unless(dry)
    }

  /**
   * SNAPSHOTS
   *
   * create a Scoobi job to copy all the snapshots paths as one big DList
   */
  private def copySnapshots(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec], dry: Boolean): ScoobiAction[Unit] = for {
    paths  <- ScoobiAction.fromHdfs(Hdfs.globPaths(from.snapshots.toHdfs))
    dlists <- paths.traverse(copySnapshot(from, to, codec))
    _      <- scoobiJob(dlists.reduce(_++_).persist(_)).unless(dry)
  } yield ()

  import FlatFactThriftStorageV1._
  private def copySnapshot(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec]) = (path: Path) =>
    loadFacts(path).flatMap(storeFacts(path, to, codec))

  private def loadFacts(path: Path): ScoobiAction[DList[Fact]] =
    scoobiJob(FlatFactThriftLoader(path.toString).loadScoobi(_)).map(_.map(throwAwayErrors("Could not load facts")))

  private def storeFacts(path: Path,to: HdfsRepository, codec: Option[CompressionCodec]) = (facts: DList[Fact]) =>
    scoobiJob(FlatFactThriftStorer(new Path(to.snapshots.toHdfs, path.getName).toString, codec).storeScoobi(facts)(_))

  private def nonEmptyFactsetsNames(from: Repository): Hdfs[Set[String]] = for {
    paths    <- Hdfs.globPaths(from.factsets.toHdfs)
    children <- paths.traverse(p => Hdfs.globFiles(p, "*/*/*/*/*").map(ps => (p, ps.isEmpty)) ||| Hdfs.value((p, true)))
  } yield children.filterNot(_._2).map(_._1.getName).toSet

  private def throwAwayErrors[E, A](message: String) = (ea: E \/ A) => ea match {
    case -\/(e) => sys.error(s"$message '$e'")
    case \/-(a) => a
  }

  /**
   * Execute a stat action and log the result
   */
  private def logStat[A](name: String, repository: Repository, stat: StatAction[A]): RecreateAction[Unit] =
    fromStat(repository, stat).log(value => s"$name in '${repository.root}' is '$value'")
}
