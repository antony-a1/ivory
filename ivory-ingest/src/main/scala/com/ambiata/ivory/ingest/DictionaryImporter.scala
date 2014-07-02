package com.ambiata.ivory.ingest

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import scalaz._, effect._

// FIX move to com.ambiata.ivory.ingest.internal
object DictionaryImporter {

  def fromPath(repository: Repository, path: FilePath, importType: ImportType): ResultTIO[FilePath] =
    DictionaryTextStorage.DictionaryTextLoader(repository, path).load.flatMap(fromDictionary(repository, _, importType))

  def fromDictionary(repository: Repository, dictionary: Dictionary, importType: ImportType): ResultTIO[FilePath] = {
    val storage = DictionaryThriftStorage(repository)
    for {
      oldDictionary <- importType match {
        case Update => storage.load
        case Override => ResultT.ok[IO, Dictionary](Dictionary(Map()))
      }
      out <- storage.store(oldDictionary.append(dictionary))
    } yield out._2
  }

  sealed trait ImportType
  case object Update extends ImportType
  case object Override extends ImportType
}
