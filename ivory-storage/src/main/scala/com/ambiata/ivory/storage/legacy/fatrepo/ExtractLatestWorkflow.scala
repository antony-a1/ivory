package com.ambiata.ivory.storage.legacy.fatrepo

import scalaz._, Scalaz._
import scala.math.{Ordering => SOrdering}
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.alien.hdfs._

/**
 * This workflow is designed to extract the latest features from a feature store
 *
 * Steps:
 * 1. Find the latest version of a feature store and dictionary.
 *    - This will get a listing of all feature stores and dictionaries, ordering them by name, and taking the last (latest)
 * 2. Extract the most recent version of every feature for every entity.
 *    - Run snapshot app with discovered store and dictionary
 *    - Store in sparse row thrift format
 */
object ExtractLatestWorkflow {

  type FeatureStoreName = String
  type DictionaryName = String
  type Extractor = (HdfsRepository, FeatureStoreName, DictionaryName, LocalDate) => ScoobiAction[Unit]

  private implicit val logger = LogFactory.getLog("ivory.repository.fatrepo.ExtractLatestWorkflow")

  def onHdfs(repoPath: Path, extractor: Extractor, date: LocalDate): ScoobiAction[(String, String)] = {
    val repo = Repository.fromHdfsPath(repoPath)
    for {
      store <- ScoobiAction.fromHdfs(latestStore(repo))
      dname <- ScoobiAction.fromHdfs(latestDictionary(repo))
      _      = logger.info(s"Running extractor on '${repo.path}' repo, '${store}' store, '${dname}' dictionary, '${date.toString("yyyy-MM-dd")}' date")
      _     <- extractor(repo, store, dname, date)
    } yield (store, dname)
  }

  def latestStore(repo: HdfsRepository): Hdfs[String] = for {
    _         <- Hdfs.value(logger.info(s"Finding latest feature store in the '${repo.path}' repository."))
    latestOpt <- latest(repo.storesPath)
    latest    <- latestOpt.map(Hdfs.ok(_)).getOrElse(Hdfs.fail(s"There are no feature stores"))
    _          = logger.info(s"Latest feature store is '${latest}'")
  } yield latest

  def latestDictionary(repo: HdfsRepository): Hdfs[String] = for {
    _         <- Hdfs.value(logger.info(s"Finding latest dictionary in the '${repo.path}' repository."))
    latestOpt <- latest(repo.dictionariesPath)
    latest    <- latestOpt.map(Hdfs.ok(_)).getOrElse(Hdfs.fail(s"There are no dictionaries'"))
    _          = logger.info(s"Latest dictionary is '${latest}'")
  } yield latest

  def latest(path: Path): Hdfs[Option[String]] =
    Hdfs.globPaths(path).map(_.map(_.getName).sorted(SOrdering[String].reverse).headOption)
}
