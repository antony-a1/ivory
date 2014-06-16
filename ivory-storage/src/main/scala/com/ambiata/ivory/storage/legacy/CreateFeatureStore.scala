package com.ambiata.ivory.storage.legacy

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path

import com.ambiata.mundane.io._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.repository._
import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import HdfsS3Action._

object CreateFeatureStore {

  def onHdfs(repoPath: Path, name: String, sets: List[Factset], existing: Option[String] = None): Hdfs[Unit] = {
     for {
       repo     <- Hdfs.configuration.map(c => Repository.fromHdfsPath(repoPath.toString.toFilePath, ScoobiConfiguration(c)))
       tmp      <- Hdfs.value(FeatureStoreTextStorage.fromFactsets(sets))
       store    <- existing.traverse(e => IvoryStorage.storeFromIvory(repo, e))
       newStore  = store.map(fs => tmp +++ fs).getOrElse(tmp)
       _        <- Hdfs.mkdir(repo.stores.toHdfs)
       _        <- IvoryStorage.storeToIvory(repo, newStore, name)
     } yield ()
   }

  def onS3(repository: S3Repository, name: String, sets: List[Factset], existing: Option[String] = None): HdfsS3Action[Unit] = {
    for {
      tmp      <- HdfsS3Action.fromHdfs(Hdfs.value(FeatureStoreTextStorage.fromFactsets(sets)))
      store    <- existing.traverse(e => IvoryStorage.storeFromIvoryS3(repository, e))
      newStore  = store.map(fs => tmp +++ fs).getOrElse(tmp)
      _        <- IvoryStorage.storeToIvoryS3(repository, newStore, name)
    } yield ()
  }

}
