package com.ambiata.ivory.storage.repository

import com.ambiata.mundane.io._
import com.ambiata.saws.core._
import com.nicta.scoobi.Scoobi._
import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path

class RepositorySpec extends Specification with ScalaCheck { def is = s2"""

Repository Known Answer Tests
-----------------------------

  Can parse hdfs URIs                             $hdfs
  Can parse s3 URIs                               $s3
  Can parse local URIs                            $local
  Can parse relative URIs                         $relative
  Can parse default local URIs                    $dfault
  Can parse default relative local URIs           $fragment

"""
  lazy val Conf = ScoobiConfiguration()

  def hdfs =
    Repository.fromUri("hdfs:///some/path", Conf).toEither must beRight((r: Repository) => r must beLike({
      case HdfsRepository(_, _, run) =>
        r must_== HdfsRepository("/some/path".toFilePath, Conf, run)
    }))

  def s3 =
    Repository.fromUri("s3://bucket/key", Conf).toEither must beRight((r: Repository) => r must beLike({
      case S3Repository(_, _, _, _, client, run) =>
        r must_== S3Repository("bucket", "key".toFilePath, Repository.defaultS3TmpDirectory, Conf, client, run)
    }))

  def local =
    Repository.fromUri("file:///some/path", Conf).toEither must beRight(LocalRepository(FilePath.root </> "some" </> "path"))

  def relative =
    Repository.fromUri("file:some/path", Conf).toEither must beRight(LocalRepository("some" </> "path"))

  def dfault =
    Repository.fromUri("/some/path", Conf).toEither must beRight(LocalRepository(FilePath.root </> "some" </> "path"))

  def fragment =
    Repository.fromUri("some/path", Conf).toEither must beRight(LocalRepository("some" </> "path"))

}
