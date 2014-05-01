package com.ambiata.ivory.snapshot

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.scoobi.WireFormats, WireFormats._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.alien.hdfs._

object Pivot {

  def onHdfs(input: Path, output: Path, errors: Path, dictionary: Path, delim: Char, tombstone: String): ScoobiAction[Unit] = for {
    d <- ScoobiAction.fromHdfs(DictionaryTextStorage.DictionaryTextLoader(dictionary).load)
    s  = DenseRowTextStorageV1.DenseRowTextStorer(output.toString, d, delim, tombstone)
    _ <- scoobiJob(input, s, errors)
    _ <- s.storeMeta
  } yield ()

  def scoobiJob(input: Path, storer: DenseRowTextStorageV1.DenseRowTextStorer, errorPath: Path): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      implicit val FactWireFormat = WireFormats.FactWireFormat

      val in: DList[String \/ Fact] = PartitionFactThriftStorageV2.PartitionedFactThriftLoader(input.toString).loadScoobi

      val errors: DList[String] = in.collect({
        case -\/(e) => e
      })

      val good: DList[Fact] = in.collect({
        case \/-(f) => f
      })

      persist(storer.storeScoobi(good), errors.toTextFile(errorPath.toString))
    })
}
