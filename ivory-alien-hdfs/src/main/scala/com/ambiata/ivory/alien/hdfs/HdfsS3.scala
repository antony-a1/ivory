package com.ambiata.ivory.alien.hdfs

import com.amazonaws.services.s3.model._
import com.ambiata.saws.s3.S3
import com.ambiata.saws.core._
import org.apache.hadoop.fs.Path
import scalaz._, Scalaz._
import HdfsS3Action._
import com.ambiata.mundane.control.ResultT
import com.amazonaws.services.s3.AmazonS3Client

object HdfsS3 {
  def putPath(bucket: String, key: String, path: Path, metadata: ObjectMetadata = S3.ServerSideEncryption)(implicit c: Client[AmazonS3Client]): HdfsS3Action[Unit] = {
    fromHdfs(Hdfs.readWith(path, is =>
      S3Action(_.putObject(bucket, key, is, metadata)).
        onResult(_.prependErrorMessage(s"Could not put file to S3://$bucket/$key")).evalT)).map((result: PutObjectResult) => ())
  }

  def putPaths(bucket: String, key: String, path: Path, glob: String = "*/*/*/*/*/*", metadata: ObjectMetadata = S3.ServerSideEncryption)(implicit c: Client[AmazonS3Client]): HdfsS3Action[Unit] =  for {
    paths <- HdfsS3Action.fromHdfs(Hdfs.globFilesRecursively(path, glob))
    _     <- paths.map(path => putPath(bucket, key+"/"+path.getName, path, metadata)).sequenceU
  } yield ()

  def putPathsByDate(bucket: String, key: String, path: Path, glob: String = "*", metadata: ObjectMetadata = S3.ServerSideEncryption)(implicit c: Client[AmazonS3Client]): HdfsS3Action[Unit] =  for {
    paths <- HdfsS3Action.fromHdfs(Hdfs.globFilesRecursively(path, glob))
    _     <- paths.groupBy(byDate).toList.flatMap { case (date, ps) =>
      ps.toList.zipWithIndex.map { case (p, i) => putPath(bucket, s"$key/$date/eavt-$i", p, metadata) }
    }.sequenceU
  } yield ()

  private def byDate(path: Path) = {
    path.toString.split("/").reverse.drop(1).take(3).reverse.mkString("/")
  }
}
