package com.ambiata.ivory.core

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
  def hdfs =
    Repository.fromUri("hdfs:///some/path").toEither must beRight(HdfsRepository(new Path("/some/path")))

  def s3 =
    Repository.fromUri("s3://bucket/key").toEither must beRight(S3Repository("bucket", "key"))

  def local =
    Repository.fromUri("file:///some/path").toEither must beRight(LocalRepository("/some/path"))

  def relative =
    Repository.fromUri("file:some/path").toEither must beRight(LocalRepository("some/path"))

  def dfault =
    Repository.fromUri("/some/path").toEither must beRight(LocalRepository("/some/path"))

  def fragment =
    Repository.fromUri("some/path").toEither must beRight(LocalRepository("some/path"))

}
