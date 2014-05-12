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
  val VersionFile = ".version"

  def read(repository: Repository, factsetId: Factset): ResultT[IO, FactsetVersion] =
    repository.toStore.utf8.read(Repository.factset(factsetId) </> VersionFile).flatMap(parse(factsetId, _))

  def write(repository: Repository, factsetId: Factset, version: FactsetVersion): ResultT[IO, Unit] =
    repository.toStore.utf8.write(Repository.factset(factsetId) </> VersionFile, version.toString)

  def readAll(repository: Repository, factsetIds: List[Factset]): ResultT[IO, List[(Factset, FactsetVersion)]] =
    factsetIds.traverseU(factsetId => read(repository, factsetId).map(factsetId -> _))

  def writeAll(repository: Repository, factsetIds: List[Factset], version: FactsetVersion): ResultT[IO, Unit] =
    factsetIds.traverseU(write(repository, _, version)).void

  def parse(factsetId: Factset, version: String): ResultT[IO, FactsetVersion] =
    FactsetVersion.fromString(version.trim) match {
      case None =>
        ResultT.fail(s"Factset version '${version}' in factset '${factsetId}' not found.")
      case Some(v) =>
        v.pure[ResultTIO]
    }
}
