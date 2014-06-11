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

object FlatFactThriftStorageV1 {

  case class FlatFactThriftLoader(path: String) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
      valueFromSequenceFile[Fact](path).map(_.right[ParseError])
  }

  case class FlatFactThriftStorer(path: String, codec: Option[CompressionCodec]) extends IvoryScoobiStorer[Fact, DList[Fact]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[Fact] = {
      val toPersist = dlist.valueToSequenceFile(path, overwrite = true)
      codec.map(toPersist.compressWith(_)).getOrElse(toPersist)
    }
  }
}
