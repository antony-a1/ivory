package com.ambiata.ivory.data

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.PosixStore
import com.ambiata.mundane.testing.ResultMatcher._
import org.specs2._
import scalaz._, Scalaz._
import scodec.bits.ByteVector
import com.ambiata.ivory.testing.DirIO

class IdentifierStorageSpec extends Specification { def is = s2"""

Identifier Storage Spec
-----------------------

  Get empty                                  $getEmpty
  Get empty fail                             $getEmptyFail
  Put and then get                           $putAndGet
  Put multiple times and then get            $putMultiple

"""

  def getEmpty = DirIO.run { dir =>
    IdentifierStorage.get(PosixStore(dir), FilePath("a"))
  } must beOkValue(Scalaz.none)

  def getEmptyFail = DirIO.run { dir =>
    IdentifierStorage.getOrFail(PosixStore(dir), FilePath("a"))
  }.toOption ==== Scalaz.none

  def putAndGet = DirIO.run { dir =>
    val args = PosixStore(dir) -> FilePath("a")
    IdentifierStorage.put(ByteVector.empty).tupled(args) >> IdentifierStorage.getOrFail.tupled(args)
  } must beOkValue(Identifier.initial -> FilePath("a/00000000"))

  def putMultiple = DirIO.run { dir =>
    val args = PosixStore(dir) -> FilePath("a")
    for {
      _ <- IdentifierStorage.put(ByteVector.empty).tupled(args)
      i1 <- IdentifierStorage.getOrFail.tupled(args)
      _ <- IdentifierStorage.put(ByteVector.empty).tupled(args)
      i2 <- IdentifierStorage.getOrFail.tupled(args)
      _ <- IdentifierStorage.put(ByteVector.empty).tupled(args)
      i3 <- IdentifierStorage.getOrFail.tupled(args)
    } yield (i1._2, i2._2, i3._2)
  } must beOkValue((FilePath("a/00000000"), FilePath("a/00000001"), FilePath("a/00000002")))
}
