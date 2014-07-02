package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._, DictionaryThriftConversion.dictionary._
import com.ambiata.ivory.data.{IdentifierStorage, Identifier}
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import scalaz._, effect._
import scodec.bits.ByteVector

case class DictionaryThriftStorage(repository: Repository)
  extends IvoryLoader[ResultTIO[Dictionary]] with IvoryStorer[Dictionary, ResultTIO[(Identifier, FilePath)]] {

  val store = repository.toStore
  // NOTE: This has to be relative to store.list() to work
  var dictDir = repository.dictionaries.relativeTo(repository.root)

  def load = for {
    dictPath <- IdentifierStorage.getOrFail(store, dictDir)
    dict <- loadFromPath(dictPath._2)
  } yield dict

  def loadFromPath(dictPath: FilePath) = for {
    bytes <- store.bytes.read(dictPath)
    dict = ThriftSerialiser().fromBytes1(() => new ThriftDictionary(), bytes.toArray)
  } yield from(dict)

  def store(dictionary: Dictionary) = for {
    bytes <- ResultT.safe[IO, Array[Byte]](ThriftSerialiser().toBytes(to(dictionary)))
    i <- IdentifierStorage.put(ByteVector(bytes))(store, dictDir)
  } yield i
}
