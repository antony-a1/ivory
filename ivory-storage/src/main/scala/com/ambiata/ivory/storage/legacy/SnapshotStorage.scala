package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress.{SnappyCodec, CompressionCodec}
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.scoobi._
import WireFormats._
import FactFormats._
import SeqSchemas._

object SnapshotStorageV1 {

  case class SnapshotLoader(path: Path) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
      valueFromSequenceFile[Fact](path.toString).map(_.right[ParseError])
  }

  case class SnapshotStorer(path: Path, codec: Option[CompressionCodec]) extends IvoryScoobiStorer[Fact, DList[Fact]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[Fact] = {
      val toPersist = dlist.valueToSequenceFile(path.toString, overwrite = true)
      codec.map(toPersist.compressWith(_)).getOrElse(toPersist)
    }
  }

  def snapshotFromHdfs(path: Path): ScoobiAction[DList[ParseError \/ Fact]] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      SnapshotLoader(path).loadScoobi
    })

  def snapshotToHdfs(dlist: DList[Fact], path: Path, codec: Option[CompressionCodec]): ScoobiAction[DList[Fact]] =
  ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration => SnapshotStorer(path, codec).storeScoobi(dlist) })
}
