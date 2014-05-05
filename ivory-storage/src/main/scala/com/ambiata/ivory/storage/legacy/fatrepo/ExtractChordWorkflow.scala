package com.ambiata.ivory.storage.legacy.fatrepo

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._

/**
 * TODO This is a generalisation of ExtractLatestWorkflow and both should be refactored
 */
object ExtractChordWorkflow {

  type FeatureStoreName = String
  type DictionaryName = String
  type Extractor = (HdfsRepository, FeatureStoreName, DictionaryName) => ScoobiAction[Unit]

  private implicit val logger = LogFactory.getLog("ivory.repository.fatrepo.ExtractChordWorkflow")

  def onHdfs(repoPath: Path, extractor: Extractor): ScoobiAction[(String, String)] = {
     for {
      repo  <- ScoobiAction.scoobiConfiguration.map(sc => Repository.fromHdfsPath(repoPath.toString.toFilePath, ScoobiRun(sc)))
      store <- ScoobiAction.fromHdfs(ExtractLatestWorkflow.latestStore(repo))
      dname <- ScoobiAction.fromHdfs(ExtractLatestWorkflow.latestDictionary(repo))
      _      = logger.info(s"Running extractor on '${repo.root.path}' repo, '${store}' store, '${dname}' dictionary'")
      _     <- extractor(repo, store, dname)
    } yield (store, dname)
  }
}
