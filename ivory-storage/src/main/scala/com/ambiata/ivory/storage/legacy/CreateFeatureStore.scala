package com.ambiata.ivory.storage.legacy

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._
import HdfsS3Action._

object CreateFeatureStore {

  def onHdfs(repoPath: Path, name: String, sets: List[String], existing: Option[String] = None): Hdfs[Unit] = {
     val repo = Repository.fromHdfsPath(repoPath)
     for {
       tmp      <- Hdfs.fromDisjunction(FeatureStoreTextStorage.fromLines(sets))
       store    <- existing.traverse(e => IvoryStorage.storeFromIvory(repo, e))
       newStore  = store.map(fs => tmp +++ fs).getOrElse(tmp)
       _        <- Hdfs.mkdir(repo.storesPath)
       _        <- IvoryStorage.storeToIvory(repo, newStore, name)
     } yield ()
   }

  def onS3(repository: S3Repository, name: String, sets: List[String], existing: Option[String] = None): HdfsS3Action[Unit] = {
    for {
      tmp      <- HdfsS3Action.fromHdfs(Hdfs.fromDisjunction(FeatureStoreTextStorage.fromLines(sets)))
      store    <- existing.traverse(e => IvoryStorage.storeFromIvory(repository, e))
      newStore  = store.map(fs => tmp +++ fs).getOrElse(tmp)
      _        <- IvoryStorage.storeToIvory(repository, newStore, name)
    } yield ()
  }

}
