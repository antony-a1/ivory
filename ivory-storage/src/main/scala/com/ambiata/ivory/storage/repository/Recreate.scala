package com.ambiata.ivory
package storage
package repository

import com.ambiata.ivory.alien.hdfs.Hdfs
import com.ambiata.ivory.core.{IvorySyntax, Factset, PrioritizedFactset, FeatureStore}
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage.legacy.FlatFactThriftStorageV1.{FlatFactThriftStorer, FlatFactThriftLoader}
import com.ambiata.ivory.storage.legacy.{FlatFactThriftStorageV1, IvoryStorage}
import com.ambiata.mundane.io.FilePath
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodec
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.FactFormats._

import scalaz.{DList => _, _}, Scalaz._, \&/._
import scalaz.effect.IO
import RecreateAction._
import IvorySyntax._

/**
 * Recreate actions for recreating parts or all of a repository
 */
object Recreate {
  def all: RecreateAction[Unit] =
    metadata.log("****** Recreating metadata") >>
    factsets.log("****** Recreating factsets") >>
    snapshots.log(s"****** Recreating snapshots")

  def metadata: RecreateAction[Unit] =
    dictionaries.log(s"****** Recreating dictionaries") >>
    stores.log(s"****** Recreating stores")

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
   * Execute a stat action and log the result
   */
  private def logStat[A](name: String, repository: Repository, stat: StatAction[A]): RecreateAction[Unit] =
    fromStat(repository, stat).log(value => s"$name in '${repository.root}' is '$value'")

  /**
   * recreate a given set of data and log before/after count and size
   */
  private def recreate[A, V](name: String, f: Repository => FilePath)(action: RecreateConfig => RecreateAction[A]): RecreateAction[Unit] =
    configuration.flatMap { conf =>
      logStat("Number of "+name, conf.from, StatAction.numberOf(f)) >>
      logStat("Size of "+name, conf.from, StatAction.sizeOf(f)) >>
        action(conf) >>
        unless (conf.dry) {
          logStat("Number of "+name, conf.from, StatAction.numberOf(f)) >>
          logStat("Size of "+name, conf.from, StatAction.sizeOf(f))
        }
    }


  private def copyDictionaries(from: HdfsRepository, to: HdfsRepository, dry: Boolean): Hdfs[Unit] = for {
    dictPaths <- Hdfs.globPaths(from.dictionaries.toHdfs)
    _         <- if (dry) Hdfs.ok(()) else Hdfs.mkdir(to.dictionaries.toHdfs)
    _         <- dictPaths.traverse(dp => {
      println(s"${from.dictionaryByName(dp.getName)} -> ${to.dictionaryByName(dp.getName)}")
      if(dry) Hdfs.ok(()) else for {
        ret <- IvoryStorage.dictionaryPartsFromIvory(from, dp.getName).map((dp.getName, _))
        (n, dicts) = ret
        _   <- IvoryStorage.dictionariesToIvory(to, dicts, n)
      } yield ()
    })
  } yield ()

  private def copyStores(from: HdfsRepository, to: HdfsRepository, clean: Boolean, dry: Boolean): Hdfs[Unit] = for {
    storePaths <- Hdfs.globFiles(from.stores.toHdfs)
    _          <- if(dry) Hdfs.ok(()) else Hdfs.mkdir(to.stores.toHdfs)
    factsets   <- Hdfs.globPaths(from.factsets.toHdfs)
    filtered   <- filterEmptyFactsets(factsets).map(_.map(_.getName).toSet)
    stores     <- storePaths.traverse(sp => for {
      _        <- Hdfs.fromIO(IO(println(s"${from.storeByName(sp.getName)} -> ${to.storeByName(sp.getName)}")))
      ret        <- IvoryStorage.storeFromIvory(from, sp.getName).map(s => (sp.getName, s))
      (n, s)     = ret
      store      = if(clean)
        FeatureStore(PrioritizedFactset.fromFactsets(s.factsets.collect({
          case PrioritizedFactset(set, _) if filtered.contains(set.name) => set
        })))                else s
      removed    = PrioritizedFactset.diff(s.factsets, store.factsets).map(_.set.name)
      _          = if (removed.nonEmpty) println(s"Removed factsets '${removed.mkString(",")}' from feature store '${n}' as they are empty.")
      _          <- if (dry) Hdfs.ok(()) else IvoryStorage.storeToIvory(to, store, n)
    } yield ())
  } yield ()

  private def copySnapshots(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec], dry: Boolean): ScoobiAction[Unit] = for {
    snapPaths <- ScoobiAction.fromHdfs(Hdfs.globPaths(from.snapshots.toHdfs))
    dlists    <- snapPaths.traverse(sp => ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      import FlatFactThriftStorageV1._
      val facts = FlatFactThriftLoader(sp.toString).loadScoobi.map({
        case -\/(e) => sys.error("Could not load facts '${e}'")
        case \/-(f) => f
      })
      FlatFactThriftStorer(new Path(to.snapshots.toHdfs, sp.getName).toString, codec).storeScoobi(facts)
    }))
    _ <- if(dry) ScoobiAction.ok(()) else ScoobiAction.scoobiJob(sc => persist(dlists.reduce(_++_))(sc))
  } yield ()

  private def copyFactsets(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec], dry: Boolean): ScoobiAction[Unit] = for {
    fpaths    <- ScoobiAction.fromHdfs(Hdfs.globPaths(from.factsets.toHdfs))
    filtered  <- ScoobiAction.fromHdfs(filterEmptyFactsets(fpaths))
    _         <- filtered.traverse(fp => for {
      factset   <- ScoobiAction.value(Factset(fp.getName))
      raw       <- IvoryStorage.factsFromIvoryFactset(from, factset)
      _         <- ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
        import IvoryStorage._
        val facts = raw.map({
          case -\/(e) => sys.error("Could not load facts '${e}'")
          case \/-(f) => f
        })
        if(!dry) persist(facts.toIvoryFactset(to, factset, codec))
      })
    } yield ())
  } yield ()

  private def filterEmptyFactsets(paths: List[Path]): Hdfs[List[Path]] = for {
    children <- paths.traverse(p => Hdfs.globFiles(p, "*/*/*/*/*").map(ps => (p, ps.isEmpty)) ||| Hdfs.value((p, true)))
  } yield children.collect({ case (p, false) => p })

}
