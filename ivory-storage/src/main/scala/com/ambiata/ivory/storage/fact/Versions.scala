package com.ambiata.ivory.storage.fact

import scalaz._, Scalaz._, effect.IO
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse._
import com.ambiata.saws.s3.S3
import com.ambiata.saws.core._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs.HdfsS3Action
import com.ambiata.ivory.alien.hdfs.HdfsS3Action._

object Versions {
  def read(repository: Repository, factset: Factset): ResultT[IO, FactsetVersion] =
    repository.toStore.utf8.read(Repository.version(factset)).flatMap(parse(factset, _))

  def write(repository: Repository, factset: Factset, version: FactsetVersion): ResultT[IO, Unit] =
    repository.toStore.utf8.write(Repository.version(factset), version.toString)

  def readAll(repository: Repository, factsets: List[Factset]): ResultT[IO, List[(Factset, FactsetVersion)]] =
    factsets.traverseU(factset => read(repository, factset).map(factset -> _))

  def writeAll(repository: Repository, factsets: List[Factset], version: FactsetVersion): ResultT[IO, Unit] =
    factsets.traverseU(write(repository, _, version)).void

  def readPrioritized(repository: Repository, factsets: List[PrioritizedFactset]): ResultT[IO, List[(PrioritizedFactset, FactsetVersion)]] =
    factsets.traverseU(factset => read(repository, factset.set).map(factset -> _))

  def parse(factset: Factset, version: String): ResultT[IO, FactsetVersion] =
    FactsetVersion.fromString(version.trim) match {
      case None =>
        ResultT.fail(s"Factset version '${version}' in factset '${factset}' not found.")
      case Some(v) =>
        v.pure[ResultTIO]
    }
}
