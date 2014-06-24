package com.ambiata.ivory.storage.repository

import com.ambiata.mundane.io.Logger
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.nicta.scoobi.Scoobi._
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.alien.hdfs._

import scalaz.{DList => _, _}, Scalaz._, \&/._
import scalaz.effect.IO

case class RecreateConfig(from: Repository, to: Repository,
                          sc: ScoobiConfiguration, codec: Option[CompressionCodec],
                          reduce: Boolean, clean: Boolean, dry: Boolean,
                          logger: Logger) {
  val (hdfsFrom, hdfsTo) = (from, to) match {
    case (f: HdfsRepository, t: HdfsRepository) => (f, t)
    case _ => sys.error(s"Repository combination '${from}' and '${to}' not supported!")
  }
}

case class RecreateAction[+A](action: ActionT[IO, Unit, RecreateConfig, A]) {
  def run(conf: RecreateConfig): ResultTIO[A] = 
    validate.flatMap(_ => this).action.executeT(conf)
  
  def validate: RecreateAction[Unit] = for {
    c <- RecreateAction.configuration
    _ <- if(c.hdfsFrom.root == c.hdfsTo.root) RecreateAction.fail(s"Repository '${c.hdfsFrom}' is the same as '${c.hdfsTo}'") else RecreateAction.ok(())
    e <- RecreateAction.fromHdfs(Hdfs.exists(c.hdfsTo.root.toHdfs))
    _ <- if(e) RecreateAction.fail(s"Repository '${c.hdfsTo.root}' already exists") else RecreateAction.ok(())
  } yield ()

  def map[B](f: A => B): RecreateAction[B] =
    RecreateAction(action.map(f))

  def flatMap[B](f: A => RecreateAction[B]): RecreateAction[B] =
    RecreateAction(action.flatMap(a => f(a).action))

  def mapError(f: These[String, Throwable] => These[String, Throwable]): RecreateAction[A] =
    RecreateAction(action.mapError(f))

  def mapErrorString(f: String => String): RecreateAction[A] =
    RecreateAction(action.mapError(_.leftMap(f)))

  def |||[AA >: A](other: RecreateAction[AA]): RecreateAction[AA] =
    RecreateAction(action ||| other.action)

  def flatten[B](implicit ev: A <:< RecreateAction[B]): RecreateAction[B] =
    flatMap(a => ev(a))
}

object RecreateAction extends ActionTSupport[IO, Unit, RecreateConfig] {

  def log(message: String) =
    configuration.flatMap((config: RecreateConfig) => fromIO(config.logger(message)))

  def configuration: RecreateAction[RecreateConfig] =
    RecreateAction(reader(identity))

  def value[A](a: A): RecreateAction[A] =
    RecreateAction(super.ok(a))

  def ok[A](a: A): RecreateAction[A] =
    value(a)

  def safe[A](a: => A): RecreateAction[A] =
    RecreateAction(super.safe(a))

  def fromIO[A](a: =>IO[A]): RecreateAction[A] =
    RecreateAction(super.fromIO(a))

  def fail[A](e: String): RecreateAction[A] =
    RecreateAction(super.fail(e))

  def fromScoobi[A](action: ScoobiAction[A]): RecreateAction[A] = for {
    c <- configuration
    a <- fromResultTIO(action.run(c.sc))
  } yield a

  def fromHdfs[A](action: Hdfs[A]): RecreateAction[A] =
    fromScoobi(ScoobiAction.fromHdfs(action))

  def fromResultTIO[A](res: ResultTIO[A]): RecreateAction[A] =
    RecreateAction(super.fromIOResult(res.run))

  def fromStat[A](repo: Repository, action: StatAction[A]): RecreateAction[A] = for {
    c <- configuration
    a <- fromResultTIO(action.run(StatConfig(c.sc.configuration, repo)))
  } yield a

  def all: RecreateAction[Unit] = for {
    _ <- log(s"****** Recreating metadata")
    _ <- metadata
    _ <- log(s"****** Recreating factsets")
    _ <- factsets
    _ <- log(s"****** Recreating snapshots")
    _ <- snapshots
  } yield ()

  def metadata: RecreateAction[Unit] = for {
    _ <- log(s"****** Recreating dictionaries")
    _ <- dictionaries
    _ <- log(s"****** Recreating stores")
    _ <- stores
  } yield ()

  def dictionaries: RecreateAction[Unit] = for {
      conf    <- configuration
      fcount  <- fromStat(conf.from, StatAction.dictionaryVersions)
      fsize   <- fromStat(conf.from, StatAction.dictionariesSize)
      _       <- log(s"Number of dictionary versions in '${conf.from.root}' is '${fcount}'")
      _       <- log(s"Size of all dictionaries in '${conf.from.root}' is '${fsize}'")
      _       <- fromHdfs(hdfsCopyDictionaries(conf.hdfsFrom, conf.hdfsTo, conf.dry))
      _       <- if(conf.dry) RecreateAction.ok(()) else for {
      tcount  <- fromStat(conf.to, StatAction.dictionaryVersions)
      tsize   <- fromStat(conf.to, StatAction.dictionariesSize)
      _       <- log(s"Number of dictionary versions in '${conf.to.root}' is '${tcount}'")
      _       <- log(s"Size of all dictionaries in '${conf.to.root}' is '${tsize}'")
    } yield ()
  } yield ()

  private def hdfsCopyDictionaries(from: HdfsRepository, to: HdfsRepository, dry: Boolean): Hdfs[Unit] = for {
    dictPaths <- Hdfs.globPaths(from.dictionaries.toHdfs)
    _         <- if(dry) Hdfs.ok(()) else Hdfs.mkdir(to.dictionaries.toHdfs)
    _         <- dictPaths.traverse(dp => {
      println(s"${from.dictionaryByName(dp.getName)} -> ${to.dictionaryByName(dp.getName)}")
      if(dry) Hdfs.ok(()) else for {
        ret <- IvoryStorage.dictionaryPartsFromIvory(from, dp.getName).map((dp.getName, _))
        (n, dicts) = ret
        _   <- IvoryStorage.dictionariesToIvory(to, dicts, n)
      } yield ()
    })
  } yield ()

  def stores: RecreateAction[Unit] = for {
      conf      <- configuration
      fcount    <- fromStat(conf.from, StatAction.storeCount)
      fsize     <- fromStat(conf.from, StatAction.storesSize)
      _         <- log(s"Number of stores in '${conf.from.root}' is '${fcount}'")
      _         <- log(s"Size of stores in '${conf.from.root}' is '${fsize}'")
      _         <- fromHdfs(hdfsCopyStores(conf.hdfsFrom, conf.hdfsTo, conf.clean, conf.dry))
      _         <- if(conf.dry) RecreateAction.ok(()) else for {
      tsize     <- fromStat(conf.to, StatAction.storesSize)
      tcount    <- fromStat(conf.to, StatAction.storeCount)
      _         <- log(s"Number of stores in '${conf.to.root}' is '${tcount}'")
      _         <- log(s"Size of stores in '${conf.to.root}' is '${tsize}'")
    } yield ()
  } yield ()

  private def hdfsCopyStores(from: HdfsRepository, to: HdfsRepository, clean: Boolean, dry: Boolean): Hdfs[Unit] = for {
      storePaths <- Hdfs.globFiles(from.stores.toHdfs)
      _          <- if(dry) Hdfs.ok(()) else Hdfs.mkdir(to.stores.toHdfs)
      factsets   <- Hdfs.globPaths(from.factsets.toHdfs)
      filtered   <- hdfsFilterEmptyFactsets(factsets).map(_.map(_.getName).toSet)
      stores     <- storePaths.traverse(sp => for {
        _        <- Hdfs.fromIO(IO(println(s"${from.storeByName(sp.getName)} -> ${to.storeByName(sp.getName)}")))
      ret        <- IvoryStorage.storeFromIvory(from, sp.getName).map(s => (sp.getName, s))
      (n, s)     = ret
      store      = if(clean)
                    FeatureStore(PrioritizedFactset.fromFactsets(s.factsets.collect({
                      case PrioritizedFactset(set, _) if filtered.contains(set.name) => set
                    })))                else s
      removed    = PrioritizedFactset.diff(s.factsets, store.factsets).map(_.set.name)
      _          = if(!removed.isEmpty) println(s"Removed factsets '${removed.mkString(",")}' from feature store '${n}' as they are empty.")
      _          <- if(dry) Hdfs.ok(()) else IvoryStorage.storeToIvory(to, store, n)
    } yield ())
  } yield ()

  def factsets: RecreateAction[Unit] = for {
      conf      <- configuration
      fcount    <- fromStat(conf.from, StatAction.factsetCount)
      fsize     <- fromStat(conf.from, StatAction.factsetsSize)
      _         <- log(s"Number of factsets in '${conf.from.root}' is '${fcount}'")
      _         <- log(s"Size of factsets in '${conf.from.root}' is '${fsize}'")
      _         <- fromScoobi(hdfsCopyFactsets(conf.hdfsFrom, conf.hdfsTo, conf.codec, conf.dry))
      _         <- if(conf.dry) RecreateAction.ok(()) else for {
      tcount    <- fromStat(conf.to, StatAction.factsetCount)
      tsize     <- fromStat(conf.to, StatAction.factsetsSize)
      _         <- log(s"Number of factsets in '${conf.to.root}' is '${tcount}'")
      _         <- log(s"Size of factsets in '${conf.to.root}' is '${tsize}'")
    } yield ()
  } yield ()

  private def hdfsCopyFactsets(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec], dry: Boolean): ScoobiAction[Unit] = for {
      fpaths    <- ScoobiAction.fromHdfs(Hdfs.globPaths(from.factsets.toHdfs))
      filtered  <- ScoobiAction.fromHdfs(hdfsFilterEmptyFactsets(fpaths))
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

  private def hdfsFilterEmptyFactsets(paths: List[Path]): Hdfs[List[Path]] = for {
    children <- paths.traverse(p => Hdfs.globFiles(p, "*/*/*/*/*").map(ps => (p, ps.isEmpty)) ||| Hdfs.value((p, true)))
  } yield children.collect({ case (p, false) => p })

  def snapshots: RecreateAction[Unit] = for {
      conf      <- configuration
      fcount    <- fromStat(conf.from, StatAction.snapshotCount)
      fsize     <- fromStat(conf.from, StatAction.snapshotsSize)
      _         <- log(s"Number of snapshots in '${conf.from.root}' is '${fcount}'")
      _         <- log(s"Size of snapshots in '${conf.from.root}' is '${fsize}'")
      snapPaths <- fromScoobi(hdfsCopySnapshots(conf.hdfsFrom, conf.hdfsTo, conf.codec, conf.dry))
      _         <- if(conf.dry) RecreateAction.ok(()) else for {
      tcount    <- fromStat(conf.to, StatAction.snapshotCount)
      tsize     <- fromStat(conf.to, StatAction.snapshotsSize)
      _         <- log(s"Number of snapshots in '${conf.to.root}' is '${tcount}'")
      _         <- log(s"Size of snapshots in '${conf.to.root}' is '${tsize}'")
    } yield ()
  } yield ()

  private def hdfsCopySnapshots(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec], dry: Boolean): ScoobiAction[Unit] = for {
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

  implicit def RecreateActionMonad: Monad[RecreateAction] = new Monad[RecreateAction] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: RecreateAction[A])(f: A => RecreateAction[B]) = m.flatMap(f)
  }
}

