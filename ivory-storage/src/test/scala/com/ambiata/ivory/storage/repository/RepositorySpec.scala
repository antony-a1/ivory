package com.ambiata.ivory.storage.repository

import com.ambiata.mundane.io._
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
  lazy val Runner = S3Run(ScoobiConfiguration())
  def hdfs =
    Repository.fromUri("hdfs:///some/path", Runner, Runner).toEither must beRight(HdfsRepository("/some/path".toFilePath, Runner))

  def s3 =
    Repository.fromUri("s3://bucket/key", Runner, Runner).toEither must beRight(S3Repository("bucket", "key".toFilePath, Repository.defaultS3TmpDirectory, Runner))

  def local =
    Repository.fromUri("file:///some/path", Runner, Runner).toEither must beRight(LocalRepository(FilePath.root </> "some" </> "path"))

  def relative =
    Repository.fromUri("file:some/path", Runner, Runner).toEither must beRight(LocalRepository("some" </> "path"))

  def dfault =
    Repository.fromUri("/some/path", Runner, Runner).toEither must beRight(LocalRepository(FilePath.root </> "some" </> "path"))

  def fragment =
    Repository.fromUri("some/path", Runner, Runner).toEither must beRight(LocalRepository("some" </> "path"))

}
