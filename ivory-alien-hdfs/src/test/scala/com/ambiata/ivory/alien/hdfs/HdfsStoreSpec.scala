package com.ambiata.ivory.alien.hdfs

import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._, \&/._, effect.IO
import scodec.bits.ByteVector
import org.specs2._, org.specs2.matcher._
import org.scalacheck.Arbitrary, Arbitrary._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.saws.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing._, ResultTIOMatcher._
import java.io.{File, FileOutputStream, ByteArrayInputStream}
import java.util.UUID


// FIX Workout how this test can be pulled out and shared with posix/s3/hdfs.
class HdfsStoreSpec extends Specification with ScalaCheck { def is = args.execute(threadsNb = 10) ^ s2"""
  Hdfs Store Usage
  ================

  list path                                       $list
  filter listed paths                             $filter
  find path in root (thirdish)                    $find
  find path in root (first)                       $findfirst
  find path in root (last)                        $findlast

  exists                                          $exists
  not exists                                      $notExists

  delete                                          $delete
  deleteAll                                       $deleteAll

  move                                            $move
  move and read                                   $moveRead
  copy                                            $copy
  copy and read                                   $copyRead
  mirror                                          $mirror

  moveTo                                          $moveTo
  copyTo                                          $copyTo
  mirrorTo                                        $mirrorTo

  checksum                                        $checksum

  read / write bytes                              $bytes

  read / write strings                            $strings

  read / write utf8 strings                       $utf8Strings

  read / write lines                              $lines

  read / write utf8 lines                         $utf8Lines

  """

  implicit val params =
    Parameters(workers = 20, minTestsOk = 40, maxSize = 10)

  val conf = new Configuration

  implicit def HdfsStoreArbitary: Arbitrary[HdfsStore] =
    Arbitrary(arbitrary[Int].map(math.abs).map(n =>
      HdfsStore(conf, FilePath.root </> "tmp" </> s"HdfsStoreSpec.${UUID.randomUUID}.${n}")))

  def list =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
       store.list(FilePath.root) must beOkValue(filepaths) })

  def filter =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      val first = filepaths.head
      val last = filepaths.last
      val expected = if (first == last) List(first) else List(first, last)
      store.filter(FilePath.root, x => x == first || x == last) must beOkLike(paths => paths must contain(allOf(expected:_*))) })

  def find =
    prop((store: HdfsStore, paths: Paths) => paths.entries.length >= 3 ==> { clean(store, paths) { filepaths =>
      val third = filepaths.drop(2).head
      store.find(FilePath.root, _ == third) must beOkValue(Some(third)) } })

  def findfirst =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      store.find(FilePath.root, x => x == filepaths.head) must beOkValue(Some(filepaths.head)) })

  def findlast =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      store.find(FilePath.root, x => x == filepaths.last) must beOkValue(Some(filepaths.last)) })

  def exists =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      filepaths.traverseU(store.exists) must beOkLike(_.forall(identity)) })

  def notExists =
    prop((store: HdfsStore, paths: Paths) => store.exists(FilePath.root </> "i really don't exist") must beOkValue(false))

  def delete =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      val first = filepaths.head
      (store.delete(first) >> filepaths.traverseU(store.exists)) must beOkLike(x => !x.head && x.tail.forall(identity)) })

  def deleteAll =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      (store.deleteAll(FilePath.root) >> filepaths.traverseU(store.exists)) must beOkLike(x => !x.tail.exists(identity)) })

  def move =
    prop((store: HdfsStore, m: Entry, n: Entry) => clean(store, Paths(m :: Nil)) { _ =>
      (store.move(m.full.toFilePath, n.full.toFilePath) >>
       store.exists(m.full.toFilePath).zip(store.exists(n.full.toFilePath))) must beOkValue(false -> true) })

  def moveRead =
    prop((store: HdfsStore, m: Entry, n: Entry) => clean(store, Paths(m :: Nil)) { _ =>
      (store.move(m.full.toFilePath, n.full.toFilePath) >>
       store.utf8.read(n.full.toFilePath)) must beOkValue(m.value.toString) })

  def copy =
    prop((store: HdfsStore, m: Entry, n: Entry) => clean(store, Paths(m :: Nil)) { _ =>
      (store.copy(m.full.toFilePath, n.full.toFilePath) >>
       store.exists(m.full.toFilePath).zip(store.exists(n.full.toFilePath))) must beOkValue(true -> true) })

  def copyRead =
    prop((store: HdfsStore, m: Entry, n: Entry) => clean(store, Paths(m :: Nil)) { _ =>
      (store.copy(m.full.toFilePath, n.full.toFilePath) >>
       store.utf8.read(m.full.toFilePath).zip(store.utf8.read(n.full.toFilePath))) must beOkLike({ case (in, out) => in must_== out }) })

  def mirror =
    prop((store: HdfsStore, paths: Paths) => clean(store, paths) { filepaths =>
      store.mirror(FilePath.root, FilePath.root </> "mirror") >> store.list(FilePath.root </> "mirror") must beOkValue(filepaths.map("mirror" </> _)) })

  def moveTo =
    prop((store: HdfsStore, alternate: HdfsStore, m: Entry, n: Entry) => clean(store, alternate, Paths(m :: Nil)) { _ =>
      (store.moveTo(alternate, m.full.toFilePath, n.full.toFilePath) >>
       store.exists(m.full.toFilePath).zip(alternate.exists(n.full.toFilePath))) must beOkValue(false -> true) })

  def copyTo =
    prop((store: HdfsStore, alternate: HdfsStore, m: Entry, n: Entry) => clean(store, alternate, Paths(m :: Nil)) { _ =>
      (store.copyTo(alternate, m.full.toFilePath, n.full.toFilePath) >>
       store.exists(m.full.toFilePath).zip(alternate.exists(n.full.toFilePath))) must beOkValue(true -> true) })

  def mirrorTo =
    prop((store: HdfsStore, alternate: HdfsStore, paths: Paths) => clean(store, alternate, paths) { filepaths =>
      store.mirrorTo(alternate, FilePath.root, FilePath.root </> "mirror") >> alternate.list(FilePath.root </> "mirror") must beOkValue(filepaths.map("mirror" </> _)) })

  def checksum =
    prop((store: HdfsStore, m: Entry) => clean(store, Paths(m :: Nil)) { _ =>
      store.checksum(m.full.toFilePath, MD5) must beOkValue(Checksum.string(m.value.toString, MD5)) })

  def bytes =
    prop((store: HdfsStore, m: Entry, bytes: Array[Byte]) => clean(store, Paths(m :: Nil)) { _ =>
      (store.bytes.write(m.full.toFilePath, ByteVector(bytes)) >> store.bytes.read(m.full.toFilePath)) must beOkValue(ByteVector(bytes)) })

  def strings =
    prop((store: HdfsStore, m: Entry, s: String) => clean(store, Paths(m :: Nil)) { _ =>
      (store.strings.write(m.full.toFilePath, s, Codec.UTF8) >> store.strings.read(m.full.toFilePath, Codec.UTF8)) must beOkValue(s) })

  def utf8Strings =
    prop((store: HdfsStore, m: Entry, s: String) => clean(store, Paths(m :: Nil)) { _ =>
      (store.utf8.write(m.full.toFilePath, s) >> store.utf8.read(m.full.toFilePath)) must beOkValue(s) })

  def lines =
    prop((store: HdfsStore, m: Entry, s: List[Int]) => clean(store, Paths(m :: Nil)) { _ =>
      (store.lines.write(m.full.toFilePath, s.map(_.toString), Codec.UTF8) >> store.lines.read(m.full.toFilePath, Codec.UTF8)) must beOkValue(s.map(_.toString)) })

  def utf8Lines =
    prop((store: HdfsStore, m: Entry, s: List[Int]) => clean(store, Paths(m :: Nil)) { _ =>
      (store.linesUtf8.write(m.full.toFilePath, s.map(_.toString)) >> store.linesUtf8.read(m.full.toFilePath)) must beOkValue(s.map(_.toString)) })

  def files(paths: Paths): List[FilePath] =
    paths.entries.map(e => e.full.toFilePath).sortBy(_.path)

  def create(store: HdfsStore, paths: Paths): ResultT[IO, Unit] =
    paths.entries.traverseU(e =>
      Hdfs.writeWith[Unit](new Path((store.base </> e.full).path), out => ResultT.safe[IO, Unit] { out.write( e.value.toString.getBytes("UTF-8")) }).run(conf)).void

  def clean[A](store: HdfsStore, paths: Paths)(run: List[FilePath] => A): A = {
    create(store, paths).run.unsafePerformIO
    try run(files(paths))
    finally store.deleteAll(FilePath.root).run.unsafePerformIO
  }

  def clean[A](store: HdfsStore, alternate: HdfsStore, paths: Paths)(run: List[FilePath] => A): A = {
    create(store, paths).run.unsafePerformIO
    try run(files(paths))
    finally (store.deleteAll(FilePath.root) >> alternate.deleteAll(FilePath.root)).run.unsafePerformIO
  }
}
