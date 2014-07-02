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
  type Extractor = (HdfsRepository, FeatureStoreName) => ScoobiAction[Unit]

  private implicit val logger = LogFactory.getLog("ivory.repository.fatrepo.ExtractChordWorkflow")

  def onHdfs(repoPath: Path, extractor: Extractor): ScoobiAction[String] = {
     for {
      repo  <- ScoobiAction.scoobiConfiguration.map(sc => Repository.fromHdfsPath(repoPath.toString.toFilePath, sc))
      store <- ScoobiAction.fromHdfs(ExtractLatestWorkflow.latestStore(repo))
      _      = logger.info(s"Running extractor on '${repo.root.path}' repo, '${store}' store")
      _     <- extractor(repo, store)
    } yield store
  }
}
