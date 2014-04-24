package com.ambiata.ivory.alien.hdfs

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.data._
import com.ambiata.mundane.store._
import java.util.UUID
import java.io.{InputStream, OutputStream}
import java.io.{PipedInputStream, PipedOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._, scalaz.stream._, scalaz.concurrent._, effect.IO, effect.Effect._, \&/._
import scodec.bits.ByteVector

case class HdfsStore(conf: Configuration, base: FilePath) extends Store[ResultTIO] with ReadOnlyStore[ResultTIO] {
  def readOnly: ReadOnlyStore[ResultTIO] =
    this

  def basePath: Path =
    new Path(base.path)

  def normalize(key: Path): String = {
    val fs = FileSystem.get(conf)
    fs.makeQualified(key).toString.replace(fs.makeQualified(basePath).toString + "/", "")
  }

  def list(prefix: FilePath): ResultT[IO, List[FilePath]] =
    hdfs { Hdfs.globFilesRecursively(new Path((base </> prefix).path)).map(_.map(normalize).sorted).map(_.map(_.toFilePath)) }

  def filter(prefix: FilePath, predicate: FilePath => Boolean): ResultT[IO, List[FilePath]] =
    list(prefix).map(_.filter(predicate))

  def find(prefix: FilePath, predicate: FilePath => Boolean): ResultT[IO, Option[FilePath]] =
    list(prefix).map(_.find(predicate))

  def exists(path: FilePath): ResultT[IO, Boolean] =
    hdfs { Hdfs.exists(new Path((base </> path).path)) }

  def delete(path: FilePath): ResultT[IO, Unit] =
    hdfs { Hdfs.delete(new Path((base </> path).path)) }

  def deleteAll(prefix: FilePath): ResultT[IO, Unit] =
    hdfs { Hdfs.deleteAll(new Path((base </> prefix).path)) }

  def move(in: FilePath, out: FilePath): ResultT[IO, Unit] =
    copy(in, out) >> delete(in)

  def copy(in: FilePath, out: FilePath): ResultT[IO, Unit] = hdfs { for {
    dir <- Hdfs.isDirectory(new Path((base </> out).path))
    _   <- if (dir) Hdfs.cp(new Path((base </> in).path), new Path((base </> out </> in.basename).path), false) else Hdfs.cp(new Path((base </> in).path), new Path((base </> out).path), false)
  } yield () }

  def mirror(in: FilePath, out: FilePath): ResultT[IO, Unit] = for {
    paths <- list(in)
    _     <- paths.traverseU({ source =>
      val destination = out </> source.path.replace(in.path + "/", "")
      copy(source, destination)
    })
  } yield ()

  def moveTo(store: Store[ResultTIO], src: FilePath, dest: FilePath): ResultT[IO, Unit] =
    copyTo(store, src, dest) >> delete(src)

  def copyTo(store: Store[ResultTIO], src: FilePath, dest: FilePath): ResultT[IO, Unit] =
    unsafe.withInputStream(src) { in =>
      store.unsafe.withOutputStream(dest) { out =>
        Streams.pipe(in, out) }}

  def mirrorTo(store: Store[ResultTIO], in: FilePath, out: FilePath): ResultT[IO, Unit] = for {
    paths <- list(in)
    _     <- paths.traverseU({ source =>
      val destination = out </> source.path.replace(in.path + "/", "")
      copyTo(store, source, destination)
    })
  } yield ()

  def checksum(path: FilePath, algorithm: ChecksumAlgorithm): ResultT[IO, Checksum] =
    withInputStreamValue[Checksum](path)(in => Checksum.stream(in, algorithm))

  val bytes: StoreBytes[ResultTIO] = new StoreBytes[ResultTIO] {
    def read(path: FilePath): ResultT[IO, ByteVector] =
      withInputStreamValue[Array[Byte]](path)(Streams.readBytes(_, 4 * 1024 * 1024)).map(ByteVector.view)

    def write(path: FilePath, data: ByteVector): ResultT[IO, Unit] =
      unsafe.withOutputStream(path)(Streams.writeBytes(_, data.toArray))

    def source(path: FilePath): Process[Task, ByteVector] =
      scalaz.stream.io.chunkR(FileSystem.get(conf).open(new Path((base </> path).path))).evalMap(_(1024 * 1024))

    def sink(path: FilePath): Sink[Task, ByteVector] =
      io.resource(Task.delay(new PipedOutputStream))(out => Task.delay(out.close))(
        out => io.resource(Task.delay(new PipedInputStream))(in => Task.delay(in.close))(
          in => Task.now((bytes: ByteVector) => Task.delay(out.write(bytes.toArray)))).toTask)
  }

  val strings: StoreStrings[ResultTIO] = new StoreStrings[ResultTIO] {
    def read(path: FilePath, codec: Codec): ResultT[IO, String] =
      bytes.read(path).map(b => new String(b.toArray, codec.name))

    def write(path: FilePath, data: String, codec: Codec): ResultT[IO, Unit] =
      bytes.write(path, ByteVector.view(data.getBytes(codec.name)))
  }

  val utf8: StoreUtf8[ResultTIO] = new StoreUtf8[ResultTIO] {
    def read(path: FilePath): ResultT[IO, String] =
      strings.read(path, Codec.UTF8)

    def write(path: FilePath, data: String): ResultT[IO, Unit] =
      strings.write(path, data, Codec.UTF8)

    def source(path: FilePath): Process[Task, String] =
      bytes.source(path) |> scalaz.stream.text.utf8Decode

    def sink(path: FilePath): Sink[Task, String] =
      bytes.sink(path).map(_.contramap(s => ByteVector.view(s.getBytes("UTF-8"))))
  }

  val lines: StoreLines[ResultTIO] = new StoreLines[ResultTIO] {
    def read(path: FilePath, codec: Codec): ResultT[IO, List[String]] =
      strings.read(path, codec).map(_.lines.toList)

    def write(path: FilePath, data: List[String], codec: Codec): ResultT[IO, Unit] =
      strings.write(path, Lists.prepareForFile(data), codec)

    def source(path: FilePath, codec: Codec): Process[Task, String] =
      scalaz.stream.io.linesR(FileSystem.get(conf).open(new Path((base </> path).path)))(codec)

    def sink(path: FilePath, codec: Codec): Sink[Task, String] =
      bytes.sink(path).map(_.contramap(s => ByteVector.view(s"$s\n".getBytes(codec.name))))
  }

  val linesUtf8: StoreLinesUtf8[ResultTIO] = new StoreLinesUtf8[ResultTIO] {
    def read(path: FilePath): ResultT[IO, List[String]] =
      lines.read(path, Codec.UTF8)

    def write(path: FilePath, data: List[String]): ResultT[IO, Unit] =
      lines.write(path, data, Codec.UTF8)

    def source(path: FilePath): Process[Task, String] =
      lines.source(path, Codec.UTF8)

    def sink(path: FilePath): Sink[Task, String] =
      lines.sink(path, Codec.UTF8)
  }

  def withInputStreamValue[A](path: FilePath)(f: InputStream => ResultT[IO, A]): ResultT[IO, A] =
    hdfs { Hdfs.readWith(new Path((base </> path).path), f) }

  val unsafe: StoreUnsafe[ResultTIO] = new StoreUnsafe[ResultTIO] {
    def withInputStream(path: FilePath)(f: InputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      withInputStreamValue[Unit](path)(f)

    def withOutputStream(path: FilePath)(f: OutputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      hdfs { Hdfs.writeWith(new Path((base </> path).path), f) }
  }

  def hdfs[A](thunk: => Hdfs[A]): ResultT[IO, A] =
    thunk.run(conf)
}
