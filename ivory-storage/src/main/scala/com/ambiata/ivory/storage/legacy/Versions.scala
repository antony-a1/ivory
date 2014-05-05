package com.ambiata.ivory.storage.legacy

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse._
import com.ambiata.saws.s3.S3
import com.ambiata.saws.core._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs.HdfsS3Action
import com.ambiata.ivory.alien.hdfs.HdfsS3Action._

sealed trait FactsetVersion
case object FactsetVersionOne extends FactsetVersion {
  override def toString = "1"
}
case object FactsetVersionTwo extends FactsetVersion {
  override def toString = "2"
}

object FactsetVersion {
  def fromString(str: String): Option[FactsetVersion] = str match {
    case "1" => Some(FactsetVersionOne)
    case "2" => Some(FactsetVersionTwo)
    case _   => None
  }
}

object Versions {

  /**
   * This method adds a .version file to each factset
   *
   * The file contains the data format version that was used to store the factset
   */
  type FactsetName = String

  val factsetVersionFile = ".version"

  def writeFactsetVersionToHdfs(repo: HdfsRepository, version: FactsetVersion, factsets: List[FactsetName]): Hdfs[Unit] = {
    factsets.traverse(fs => for {
      p <- Hdfs.value((repo.factsetById(fs) </> factsetVersionFile).toHdfs)
      e <- Hdfs.exists(p)
      _ <- if (e) Hdfs.fail(s"Path $p already exists!") else Hdfs.ok(())
    } yield p).flatMap(_.traverseU(p =>
      Hdfs.writeWith(p, os => Streams.write(os, s"${version}\n"))
    ).map(_ => ()))
  }

  def readFactsetVersionsFromHdfs(repo: HdfsRepository, factsets: List[FactsetName]): Hdfs[List[(FactsetName, FactsetVersion)]] =
    factsets.traverse(fs => readFactsetVersionFromHdfs(repo, fs).map((fs, _)))

  def readFactsetVersionFromHdfs(repo: HdfsRepository, factset: String): Hdfs[FactsetVersion] = for {
    raw <- Hdfs.readWith((repo.factsetById(factset) </> factsetVersionFile).toHdfs, is => Streams.read(is))
    v   <- Hdfs.fromDisjunction(parseFactsetVersion(raw, repo.root.path, factset))
  } yield v

  def readFactsetVersionFromS3(repo: S3Repository, factset: String): S3Action[FactsetVersion] = for {
    raw <- S3.getString(repo.bucket, (repo.factsetById(factset) </> factsetVersionFile).path)
    v   <- Aws.fromDisjunctionString(parseFactsetVersion(raw, "s3://" + repo.bucket + "/" + repo.root.path, factset))
  } yield v

  def parseFactsetVersion(raw: String, repo: String, factset: String): String \/ FactsetVersion =
    FactsetVersion.fromString(raw.trim).map(_.right).getOrElse(s"Factset version '${raw}' in factset '${factset}', repo '${repo}' not found.".left)
}
