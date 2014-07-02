package com.ambiata.ivory.data

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._

import scalaz.{Store => _, _}, Scalaz._, effect.IO
import scodec.bits.ByteVector

// TODO add streaming support.
sealed trait Metadata[F[_]] {
  def put(key: Key, value: ByteVector): F[Identifier]
  def get(identifier: Identifier, key: Key): F[ByteVector]
  def resolve(ref: Ref): F[Identifier]
}

case class StoreMetadata(store: Store[ResultTIO]) extends Metadata[ResultTIO] {
  val refs = "refs".toFilePath
  val tags = "tags".toFilePath
  val data = "data".toFilePath
  val meta = "meta".toFilePath
  val stage = "stage".toFilePath

  def put(key: Key, value: ByteVector): ResultT[IO, Identifier] =
     IdentifierStorage.put(value)(store, data </> key.render).map(_._1)

  def get(identifier: Identifier, key: Key): ResultT[IO, ByteVector] =
    store.bytes.read(data </> key.render </> identifier.render)

  def resolve(ref: Ref): ResultT[IO, Identifier] = ref match {
    case Head => for {
      e <- store.exists(refs </> "HEAD")
      c <- if (e) store.utf8.read(refs </> "HEAD").flatMap(x => Identifier.parse(x) match {
        case None =>
          ResultT.fail[IO, Identifier](s"Invalid HEAD reference [$x].")
        case Some(i) =>
          ResultT.ok[IO, Identifier](i)
      }) else ResultT.fail[IO, Identifier]("No HEAD reference could be found.")
    } yield c
    case TagRef(tag) => for {
      e <- store.exists(refs </> tag.render)
      c <- if (e) store.utf8.read(refs </> tag.render).flatMap(x => Identifier.parse(x) match {
        case None =>
          ResultT.fail[IO, Identifier](s"Invalid tag reference [$x] for tag [$tag].")
        case Some(i) =>
          ResultT.ok[IO, Identifier](i)
      }) else ResultT.fail[IO, Identifier](s"No tag [$tag] reference could be found.")
    } yield c
    case IdentifierRef(i) =>
      ResultT.ok[IO, Identifier](i)
  }
}

case class Commit(id: Identifier, entries: List[(Key, Identifier)])

object Commit {
  def encode(commit: Commit): String =
    s"""${commit.id}
       |${commit.entries.map({ case (k, i) => s"$k|$i" }).mkString("\n")}
       |""".stripMargin

  def decode(s: String): Option[Commit] =
    s.lines.toList match {
      case h :: t => for {
        id <- Identifier.parse(h)
        entries <- t.traverse(s => s.split("\\|").toList match {
          case k :: i :: Nil => for {
            kk <- Key.create(k)
            ii <- Identifier.parse(i)
          } yield (kk, ii)
          case _ =>
            None
        })
      } yield Commit(id, entries)
      case Nil =>
        None
    }

}
