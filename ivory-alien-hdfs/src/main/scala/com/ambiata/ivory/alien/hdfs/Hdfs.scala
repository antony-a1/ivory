package com.ambiata.ivory.alien.hdfs

import scalaz._, Scalaz._, \&/._, effect._, Effect._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import java.io._

import com.ambiata.mundane.control._

case class Hdfs[+A](action: ActionT[IO, Unit, Configuration, A]) {

  def run(conf: Configuration): ResultTIO[A] =
    action.executeT(conf)

  def map[B](f: A => B): Hdfs[B] =
    Hdfs(action.map(f))

  def flatMap[B](f: A => Hdfs[B]): Hdfs[B] =
    Hdfs(action.flatMap(a => f(a).action))

  def mapError(f: These[String, Throwable] => These[String, Throwable]): Hdfs[A] =
    Hdfs(action.mapError(f))

  def mapErrorString(f: String => String): Hdfs[A] =
    Hdfs(action.mapError(_.leftMap(f)))

  def |||[AA >: A](other: Hdfs[AA]): Hdfs[AA] =
    Hdfs(action ||| other.action)
}

object Hdfs extends ActionTSupport[IO, Unit, Configuration] {

  def value[A](a: A): Hdfs[A] =
    Hdfs(super.ok(a))

  def ok[A](a: A): Hdfs[A] =
    value(a)

  def safe[A](a: => A): Hdfs[A] =
    Hdfs(super.safe(a))

  def fail[A](e: String): Hdfs[A] =
    Hdfs(super.fail(e))

  def fromDisjunction[A](d: String \/ A): Hdfs[A] = d match {
    case -\/(e) => fail(e)
    case \/-(a) => ok(a)
  }

  def fromValidation[A](v: Validation[String, A]): Hdfs[A] =
    fromDisjunction(v.disjunction)

  def fromIO[A](io: IO[A]): Hdfs[A] =
    Hdfs(super.fromIO(io))

  def fromResultTIO[A](res: ResultTIO[A]): Hdfs[A] =
    Hdfs(super.fromIOResult(res.run))

  def filesystem: Hdfs[FileSystem] =
    Hdfs(reader((c: Configuration) => FileSystem.get(c)))

  def configuration: Hdfs[Configuration] =
    Hdfs(reader(identity))

  def exists(p: Path): Hdfs[Boolean] =
    filesystem.map(fs => fs.exists(p))

  def mustexist(p: Path): Hdfs[Unit] =
    exists(p).flatMap(e => if(e) Hdfs.ok(()) else Hdfs.fail(s"$p doesn't exist!"))

  def globPaths(p: Path, glob: String = "*"): Hdfs[List[Path]] =
    filesystem.map(fs =>
      if(fs.isFile(p)) List(p) else fs.globStatus(new Path(p, glob)).toList.map(_.getPath)
    )

  def globPathsRecursively(p: Path, glob: String = "*"): Hdfs[List[Path]] = {
    def getPaths(path: Path): FileSystem => List[Path] = { fs: FileSystem =>
      if (fs.isFile(path)) List(path)
      else {
        val paths = fs.globStatus(new Path(path, glob)).toList.map(_.getPath)
        (paths ++ paths.flatMap(p1 => fs.listStatus(p1).flatMap(s => getPaths(s.getPath)(fs)))).distinct
      }
    }
    filesystem.map(getPaths(p))
  }

  def globFiles(p: Path, glob: String = "*"): Hdfs[List[Path]] = for {
    fs    <- filesystem
    files <- globPaths(p, glob)
  } yield files.filter(fs.isFile)

  def globFilesRecursively(p: Path, glob: String = "*"): Hdfs[List[Path]] = for {
    fs    <- filesystem
    files <- globPathsRecursively(p, glob)
  } yield files.filter(fs.isFile)

  def readWith[A](p: Path, f: InputStream => ResultT[IO, A], glob: String = "*"): Hdfs[A] = for {
    _     <- mustexist(p)
    paths <- globFiles(p, glob)
    a     <- filesystem.flatMap(fs => {
               if(!paths.isEmpty) {
                 val is = paths.map(fs.open).reduce[InputStream]((a, b) => new SequenceInputStream(a, b))
                 Hdfs.fromResultTIO(ResultT.using(ResultT.safe[IO, InputStream](is)) { in =>
                   f(is)
                 })
               } else {
                 Hdfs.fail[A](s"No files found for path $p!")
               }
             })
  } yield a

  def writeWith[A](p: Path, f: OutputStream => ResultT[IO, A]): Hdfs[A] = for {
    _ <- mustexist(p) ||| mkdir(p.getParent)
    a <- filesystem.flatMap(fs =>
      Hdfs.fromResultTIO(ResultT.using(ResultT.safe[IO, OutputStream](fs.create(p))) { out =>
        f(out)
      }))
  } yield a

  def cp(src: Path, dest: Path, overwrite: Boolean): Hdfs[Unit] = for {
    fs   <- filesystem
    conf <- configuration
    res  <- Hdfs.value(FileUtil.copy(fs, src, fs, dest, false, overwrite, conf))
    _    <- if(!res && overwrite) fail(s"Could not copy $src to $dest") else ok(())
  } yield ()

  def mkdir(p: Path): Hdfs[Boolean] =
    filesystem.map(fs => fs.mkdirs(p))

  implicit def HdfsMonad: Monad[Hdfs] = new Monad[Hdfs] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: Hdfs[A])(f: A => Hdfs[B]) = m.flatMap(f)
  }
}
