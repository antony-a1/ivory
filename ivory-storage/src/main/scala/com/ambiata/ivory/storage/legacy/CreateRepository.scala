package com.ambiata.ivory.storage.legacy

import org.apache.hadoop.fs.Path

import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.saws.core._
import com.ambiata.saws.s3._
import java.io.File

object CreateRepository {

  def onHdfs(path: Path): Hdfs[Boolean] = {
    val meta = new Path(path, "metadata")
    val dict = new Path(meta, "dictionaries")
    val store = new Path(meta, "stores")
    val factsets = new Path(path, "factsets")
    for {
      e <- Hdfs.exists(path)
      r <- if(e) Hdfs.ok(false) else for {
        _ <- Hdfs.mkdir(dict)
        _ <- Hdfs.mkdir(store)
        _ <- Hdfs.mkdir(factsets)
      } yield true
    } yield r
  }

  def onS3(repository: S3Repository): S3Action[S3Repository] = {
    def create(name: String) =
    for {
      newFile <- S3Action.ok({val f = new File(name); f.createNewFile; f})
      _       <- S3.putFile(repository.bucket, repository.root.path, newFile)
      _       <- S3Action.ok(newFile.delete)
    } yield ()

    for {
      _ <- create(repository.dictionaries.path)
      _ <- create(repository.factsets.path)
      _ <- create(repository.stores.path)
    } yield repository
  }

}
