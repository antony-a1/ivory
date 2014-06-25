package com.ambiata.ivory.storage.repository

import com.ambiata.mundane.io.FilePath
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.alien.hdfs._

import scalaz._, Scalaz._, \&/._
import scalaz.effect.IO

case class StatConfig(conf: Configuration, repo: Repository)

case class StatAction[+A](action: ActionT[IO, Unit, StatConfig, A]) {
  def run(conf: StatConfig): ResultTIO[A] = 
    action.executeT(conf)

  def map[B](f: A => B): StatAction[B] =
    StatAction(action.map(f))

  def flatMap[B](f: A => StatAction[B]): StatAction[B] =
    StatAction(action.flatMap(a => f(a).action))

  def mapError(f: These[String, Throwable] => These[String, Throwable]): StatAction[A] =
    StatAction(action.mapError(f))

  def mapErrorString(f: String => String): StatAction[A] =
    StatAction(action.mapError(_.leftMap(f)))

  def |||[AA >: A](other: StatAction[AA]): StatAction[AA] =
    StatAction(action ||| other.action)

  def flatten[B](implicit ev: A <:< StatAction[B]): StatAction[B] =
    flatMap(a => ev(a))
}

object StatAction extends ActionTSupport[IO, Unit, StatConfig] {

  def configuration: StatAction[Configuration] =
    StatAction(reader(_.conf))

  def repository: StatAction[Repository] =
    StatAction(reader(_.repo))

  def value[A](a: A): StatAction[A] =
    StatAction(super.ok(a))

  def ok[A](a: A): StatAction[A] =
    value(a)

  def safe[A](a: => A): StatAction[A] =
    StatAction(super.safe(a))

  def fail[A](e: String): StatAction[A] =
    StatAction(super.fail(e))

  def fromHdfs[A](action: Hdfs[A]): StatAction[A] = for {
    c <- configuration
    a <- fromResultTIO(action.run(c))
  } yield a

  def fromResultTIO[A](res: ResultTIO[A]): StatAction[A] =
    StatAction(super.fromIOResult(res.run))

  def repositorySize: StatAction[Long] =
    (factsetsSize |@| metadataSize |@| snapshotsSize)(_ + _ + _)

  def metadataSize: StatAction[Long] =
    (dictionariesSize |@| storesSize)(_ + _)

  def factsetSize(factset: Factset): StatAction[Long] = repository.flatMap({
    case r: HdfsRepository => StatAction.fromHdfs(hdfsPathSize(r.factset(factset).toHdfs))
    case _                 => fail("Unsupported repository!")
  })

  def factsetFiles(factset: Factset): StatAction[Int] = repository.flatMap({
    case r: HdfsRepository => StatAction.fromHdfs(hdfsPathFiles(r.factset(factset).toHdfs))
    case _                 => fail("Unsupported repository!")
  })

  def sizeOf(path: Repository => FilePath): StatAction[Long] = repository.flatMap({
    case r: HdfsRepository => StatAction.fromHdfs(hdfsPathSize(path(r).toHdfs))
    case _                 => fail("Unsupported repository!")
  })

  def numberOf(path: Repository => FilePath): StatAction[Int] = repository.flatMap({
    case r: HdfsRepository => StatAction.fromHdfs(Hdfs.globPaths(path(r).toHdfs).map(_.size))
    case _                 => fail("Unsupported repository!")
  })

  def dictionariesSize: StatAction[Long]  = sizeOf((_:Repository).dictionaries)
  def dictionaryVersions: StatAction[Int] = numberOf((_:Repository).dictionaries)

  def storesSize: StatAction[Long] = sizeOf((_:Repository).stores)
  def storeCount: StatAction[Int]  = numberOf((_:Repository).stores)

  def factsetsSize: StatAction[Long] = sizeOf((_:Repository).factsets)
  def factsetCount: StatAction[Int] = numberOf((_:Repository).factsets)

  def snapshotsSize: StatAction[Long] = sizeOf((_:Repository).snapshots)
  def snapshotCount: StatAction[Int]  = numberOf((_:Repository).snapshots)

  private def hdfsPathSize(path: Path): Hdfs[Long] = for {
    files <- Hdfs.globFilesRecursively(path)
    sizes <- files.traverse(Hdfs.size)
  } yield sizes.foldLeft(0l)(_+_)

  private def hdfsPathFiles(path: Path): Hdfs[Int] =
    Hdfs.globFilesRecursively(path).map(_.size)

  implicit def StatActionMonad: Monad[StatAction] = new Monad[StatAction] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: StatAction[A])(f: A => StatAction[B]) = m.flatMap(f)
  }
}
