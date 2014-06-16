package com.ambiata.ivory.ingest

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.mundane.io.FilePath
import HdfsS3Action._

// FIX move to com.ambiata.ivory.ingest.internal
object FeatureStoreImporter {

   def onHdfs(repository: HdfsRepository, name: String, storePath: Path): Hdfs[Unit] = {
     for {
       s <- FeatureStoreTextStorage.storeFromHdfs(storePath)
       _ <- IvoryStorage.storeToIvory(repository, s, name)
     } yield ()
   }

  def onS3(repository: S3Repository, name: String, storePath: FilePath): HdfsS3Action[Unit] = {
    for {
      store <- HdfsS3Action.fromHdfs(FeatureStoreTextStorage.storeFromHdfs(new Path(storePath.path)))
      _     <- IvoryStorage.storeToIvoryS3(repository, store, name)
    } yield ()
  }

}
