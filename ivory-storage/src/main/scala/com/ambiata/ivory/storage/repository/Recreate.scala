package com.ambiata.ivory.storage.repository

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

case class RecreateConfig(from: Repository, to: Repository, sc: ScoobiConfiguration, codec: Option[CompressionCodec] = None, reduce: Boolean = false, dry: Boolean = false) {
  val (hdfsFrom, hdfsTo) = (from, to) match {
    case (f: HdfsRepository, t: HdfsRepository) => (f, t)
    case _ => sys.error(s"Repository combination '${from}' and '${to}' not supported!")
  }

  def withReduce(r: Boolean): RecreateConfig =
    copy(reduce = r)

  def withCodec(c: CompressionCodec): RecreateConfig =
    copy(codec = Some(c))
}

case class RecreateAction[+A](action: ActionT[IO, Unit, RecreateConfig, A]) {
  def run(conf: RecreateConfig): ResultTIO[A] = 
    validate.flatMap(_ => this).action.executeT(conf)
  
  def validate: RecreateAction[Unit] = for {
    c <- RecreateAction.configuration
    _ <- if(c.hdfsFrom.root == c.hdfsTo.root) RecreateAction.fail(s"Repository '${c.hdfsFrom}' is the same as '${c.hdfsTo}'") else RecreateAction.ok(())
    e <- RecreateAction.fromHdfs(Hdfs.exists(c.hdfsTo.root.toHdfs))
    _ <- if(e) RecreateAction.fail(s"Repository '${c.hdfsTo}' already exists") else RecreateAction.ok(())
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

  def configuration: RecreateAction[RecreateConfig] =
    RecreateAction(reader(identity))

  def value[A](a: A): RecreateAction[A] =
    RecreateAction(super.ok(a))

  def ok[A](a: A): RecreateAction[A] =
    value(a)

  def safe[A](a: => A): RecreateAction[A] =
    RecreateAction(super.safe(a))

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
    _ <- value(println(s"****** Recreating metadata")) 
    _ <- metadata
    _ <- value(println(s"****** Recreating factsets")) 
    _ <- factsets
    _ <- value(println(s"****** Recreating snapshots")) 
    _ <- snapshots
  } yield ()

  def metadata: RecreateAction[Unit] = for {
    _ <- value(println(s"****** Recreating dictionaries")) 
    _ <- dictionaries
    _ <- value(println(s"****** Recreating stores")) 
    _ <- stores
  } yield ()

  def dictionaries: RecreateAction[Unit] = for {
    conf   <- configuration
    fcount <- fromStat(conf.from, StatAction.dictionaryVersions)
    fsize  <- fromStat(conf.from, StatAction.dictionariesSize)
    _ = println(s"Number of dictionary versions in '${conf.from}' is '${fcount}'")
    _ = println(s"Size of all dictionaries in '${conf.from}' is '${fsize}'")
    _      <- fromHdfs(for {
      dictPaths <- Hdfs.globPaths(conf.hdfsFrom.dictionaries.toHdfs)
      _         <- if(conf.dry) Hdfs.ok(()) else Hdfs.mkdir(conf.hdfsTo.dictionaries.toHdfs)
      _         <- dictPaths.traverse(dp => {
        println(s"${conf.hdfsFrom.dictionaryByName(dp.getName)} -> ${conf.hdfsTo.dictionaryByName(dp.getName)}")
        if(conf.dry) Hdfs.ok(()) else for {
          ret <- IvoryStorage.dictionaryPartsFromIvory(conf.hdfsFrom, dp.getName).map((dp.getName, _))
          (n, dicts) = ret
          _   <- IvoryStorage.dictionariesToIvory(conf.hdfsTo, dicts, n)
        } yield ()
      })
    } yield ())
    _      <- if(conf.dry) RecreateAction.ok(()) else for {
      tcount <- fromStat(conf.to, StatAction.dictionaryVersions)
      tsize  <- fromStat(conf.to, StatAction.dictionariesSize)
      _ = println(s"Number of dictionary versions in '${conf.to}' is '${tcount}'")
      _ = println(s"Size of all dictionaries in '${conf.to}' is '${tsize}'")
    } yield ()
  } yield ()

  def stores: RecreateAction[Unit] = for {
    conf   <- configuration
    fcount <- fromStat(conf.from, StatAction.storeCount)
    fsize  <- fromStat(conf.from, StatAction.storesSize)
    _ = println(s"Number of stores in '${conf.from}' is '${fcount}'")
    _ = println(s"Size of stores in '${conf.from}' is '${fsize}'")
    _      <- fromHdfs(for {
      storePaths <- Hdfs.globFiles(conf.hdfsFrom.stores.toHdfs)
      _          <- if(conf.dry) Hdfs.ok(()) else Hdfs.mkdir(conf.hdfsTo.stores.toHdfs)
      stores     <- storePaths.traverse(sp => {
        println(s"${conf.hdfsFrom.storeByName(sp.getName)} -> ${conf.hdfsTo.storeByName(sp.getName)}")
        if(conf.dry) Hdfs.ok(()) else for {
          ret <- IvoryStorage.storeFromIvory(conf.hdfsFrom, sp.getName).map(s => (sp.getName, s))
          (n, s) = ret
          _   <- IvoryStorage.storeToIvory(conf.hdfsTo, s, n)
        } yield ()
      })
    } yield ())
    _      <- if(conf.dry) RecreateAction.ok(()) else for {
      tsize  <- fromStat(conf.to, StatAction.storesSize)
      tcount <- fromStat(conf.to, StatAction.storeCount)
      _ = println(s"Number of stores in '${conf.to}' is '${tcount}'")
      _ = println(s"Size of stores in '${conf.to}' is '${tsize}'")
    } yield ()
  } yield ()

  def factsets: RecreateAction[Unit] = for {
    conf         <- configuration
    fcount       <- fromStat(conf.from, StatAction.factsetCount)
    fsize        <- fromStat(conf.from, StatAction.factsetsSize)
    _ = println(s"Number of factsets in '${conf.from}' is '${fcount}'")
    _ = println(s"Size of factsets in '${conf.from}' is '${fsize}'")
    factsetPaths <- fromHdfs(Hdfs.globPaths(conf.hdfsFrom.factsets.toHdfs))
    dlists       <- fromScoobi(factsetPaths.traverse(fp => for {
      factset <- ScoobiAction.value(Factset(fp.getName))
      raw     <- IvoryStorage.factsFromIvoryFactset(conf.hdfsFrom, factset)
      dlist   <- ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
        import IvoryStorage._
        val facts = raw.map({
          case -\/(e) => sys.error("Could not load facts '${e}'")
          case \/-(f) => f
        })
        facts.toIvoryFactset(conf.hdfsTo, factset, conf.codec)
      })
    } yield dlist))
    _            <- if(conf.dry) RecreateAction.ok(()) else for {
      _      <- fromScoobi(ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration => persist(dlists.reduce(_++_)) }))
      tcount <- fromStat(conf.to, StatAction.factsetCount)
      tsize  <- fromStat(conf.to, StatAction.factsetsSize)
      _ = println(s"Number of factsets in '${conf.to}' is '${tcount}'")
      _ = println(s"Size of factsets in '${conf.to}' is '${tsize}'")
    } yield ()
  } yield ()

  def snapshots: RecreateAction[Unit] = for {
    conf      <- configuration
    fcount    <- fromStat(conf.from, StatAction.snapshotCount)
    fsize     <- fromStat(conf.from, StatAction.snapshotsSize)
    _ = println(s"Number of snapshots in '${conf.from}' is '${fcount}'")
    _ = println(s"Size of snapshots in '${conf.from}' is '${fsize}'")
    snapPaths <- fromHdfs(Hdfs.globPaths(conf.hdfsFrom.snapshots.toHdfs))
    dlists    <- snapPaths.traverse(sp => fromScoobi(ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      import FlatFactThriftStorageV1._
      val facts = FlatFactThriftLoader(sp.toString).loadScoobi.map({
        case -\/(e) => sys.error("Could not load facts '${e}'")
        case \/-(f) => f
      })
      FlatFactThriftStorer(new Path(conf.hdfsTo.snapshots.toHdfs, sp.getName).toString, conf.codec).storeScoobi(facts)
    })))
    _         <- if(conf.dry) RecreateAction.ok(()) else for {
      _      <- fromScoobi(ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration => persist(dlists.reduce(_++_)) }))
      tcount <- fromStat(conf.to, StatAction.snapshotCount)
      tsize  <- fromStat(conf.to, StatAction.snapshotsSize)
      _ = println(s"Number of snapshots in '${conf.to}' is '${tcount}'")
      _ = println(s"Size of snapshots in '${conf.to}' is '${tsize}'")
    } yield ()
  } yield ()

  implicit def RecreateActionMonad: Monad[RecreateAction] = new Monad[RecreateAction] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: RecreateAction[A])(f: A => RecreateAction[B]) = m.flatMap(f)
  }
}

