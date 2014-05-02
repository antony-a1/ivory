package com.ambiata.ivory.storage.legacy.fatrepo

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.ScoobiAction

/**
 * TODO This is a generalisation of ExtractLatestWorkflow and both should be refactored
 */
object ExtractChordWorkflow {

  type FeatureStoreName = String
  type DictionaryName = String
  type Extractor = (HdfsRepository, FeatureStoreName, DictionaryName) => ScoobiAction[Unit]

  private implicit val logger = LogFactory.getLog("ivory.repository.fatrepo.ExtractChordWorkflow")

  def onHdfs(repoPath: Path, extractor: Extractor): ScoobiAction[(String, String)] = {
    val repo = Repository.fromHdfsPath(repoPath)
    for {
      store <- ScoobiAction.fromHdfs(ExtractLatestWorkflow.latestStore(repo))
      dname <- ScoobiAction.fromHdfs(ExtractLatestWorkflow.latestDictionary(repo))
      _      = logger.info(s"Running extractor on '${repo.path}' repo, '${store}' store, '${dname}' dictionary'")
      _     <- extractor(repo, store, dname)
    } yield (store, dname)
  }
}
