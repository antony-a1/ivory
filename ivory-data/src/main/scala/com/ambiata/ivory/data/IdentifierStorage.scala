package com.ambiata.ivory.data

import com.ambiata.mundane.control._
import com.ambiata.mundane.io.FilePath
import com.ambiata.mundane.store.Store
import scalaz._, effect._
import scodec.bits.ByteVector

object IdentifierStorage {

  // NOTE: The FilePath must be _relative_ to the Store
  type StoreKeeper[A] = (Store[ResultTIO], FilePath) => ResultTIO[A]
  type IdentityPath = (Identifier, FilePath)

  def get: StoreKeeper[Option[IdentityPath]] = { (store, dir) =>
    store.list(dir)
      .map(_.flatMap(x => Identifier.parse(x.relativeTo(dir).path)).sorted.lastOption)
      .map(_.map(i => i -> (dir </> i.render)))
  }

  def getOrFail: StoreKeeper[IdentityPath] =
    (store, dir) => get(store, dir)
      .flatMap(_.fold(ResultT.fail[IO, IdentityPath](s"No identifiers found in $dir"))(ResultT.ok))

  def inc(f: StoreKeeper[Unit]): StoreKeeper[IdentityPath] = { (store, dir) =>
    val uuid = java.util.UUID.randomUUID.toString
    val from = FilePath("tmp") </> uuid
    def next = for {
      i <- get(store, dir).map(_.map(_._1).flatMap(_.next).getOrElse(Identifier.initial))
      path = dir </> i.render
      // TODO This is currently not threadsafe - need to deal with concurrent moves!
      _ <- store.move(from, path)
    } yield (i, path)
    for {
      _ <- f(store, from)
      i <- ResultTUtil.retry(5)(next)
    } yield i
  }

  def put(value: ByteVector): StoreKeeper[IdentityPath] = inc {
    (store, from) => store.bytes.write(from, value)
  }
}
