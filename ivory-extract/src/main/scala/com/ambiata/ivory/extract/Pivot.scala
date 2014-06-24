package com.ambiata.ivory.extract

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.scoobi._
import WireFormats._
import FactFormats._
import SeqSchemas._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._

/**
 * Takes a snapshot containing EAVTs
 *
 * and create a "dense" file where there is one line per entity id and all the values for that entity
 */
object Pivot {

  def onHdfsFromSnapshot(repoPath: Path, output: Path, errors: Path, delim: Char, tombstone: String, date: Date, codec: Option[CompressionCodec]): ScoobiAction[Unit] = for {
    repo <- ScoobiAction.scoobiConfiguration.map(sc => Repository.fromHdfsPath(repoPath.toString.toFilePath, sc))
    snap <- HdfsSnapshot.takeSnapshot(repoPath, errors, date, true, codec)
    (store, dname, path) = snap
    _    <- onHdfs(path, output, errors, repo.dictionaryByName(dname).toHdfs, delim, tombstone)
  } yield ()

  def onHdfs(input: Path, output: Path, errors: Path, dictionary: Path, delim: Char, tombstone: String): ScoobiAction[Unit] = for {
    d <- ScoobiAction.fromHdfs(DictionaryTextStorage.DictionaryTextLoader(dictionary).load)
    _ <- onHdfsWithDictionary(input, output, errors, d, delim, tombstone)
  } yield ()

  def onHdfsWithDictionary(input: Path, output: Path, errors: Path, dictionary: Dictionary, delim: Char, tombstone: String): ScoobiAction[Unit] = {
    val s = DenseRowTextStorageV1.DenseRowTextStorer(output.toString, dictionary, delim, tombstone)
    for {
      _ <- scoobiJob(input, s, errors)
      _ <- s.storeMeta
    } yield ()
  }

  def scoobiJob(input: Path, storer: DenseRowTextStorageV1.DenseRowTextStorer, errorPath: Path): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob { implicit sc: ScoobiConfiguration =>
      val facts = valueFromSequenceFile[Fact](input.toString)
      persist(storer.storeScoobi(facts))
    }
}
