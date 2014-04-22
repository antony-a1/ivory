package com.ambiata.ivory.example.fatrepo

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._
import com.ambiata.ivory.ingest.{DictionaryImporter, EavtTextImporter}
import com.ambiata.ivory.repository._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.alien.hdfs._
import org.joda.time.DateTimeZone

object ImportWorkflow {

  private implicit val logger = LogFactory.getLog("ivory.example.fatrepo.ImportWorkflow")

  def onHdfs(repo: Path, dict: Path, namespace: String, input: Path, tombstone: List[String], tmp: Path, errors: Path, timezone: DateTimeZone): ScoobiAction[String] =
    fatrepo.ImportWorkflow.onHdfs(repo, Some(importDictionary(dict)), importFeed(input, namespace), tombstone, tmp, errors, timezone)

  def importDictionary(path: Path)(repo: HdfsRepository, name: String, tombstone: List[String], tmpPath: Path): Hdfs[Unit] =
    DictionaryImporter.onHdfs(repo.path, path, name)

  def importFeed(input: Path, namespace: String)(repo: HdfsRepository, factset: String, dname: String, tmpPath: Path, errorPath: Path, timezone: DateTimeZone): ScoobiAction[Unit] = for {
    dict <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dname))
    _    <- EavtTextImporter.onHdfs(repo, dict, factset, namespace, input, errorPath, timezone, None)
  } yield ()
}
