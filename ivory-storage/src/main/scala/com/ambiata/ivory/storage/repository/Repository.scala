package com.ambiata.ivory.storage.repository

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.ivory.core._

sealed trait Repository {
  def root: FilePath
  def errors: FilePath = root </> "errors"
  def factsets: FilePath = root </> "factsets"
  def metadata: FilePath = root </> "metadata"
  def dictionaries: FilePath = metadata </> "dictionaries"
  def stores: FilePath = metadata </> "stores"
  def dictionaryByName(name: String): FilePath =  dictionaries </> name
  def storeByName(name: String): FilePath =  stores </> name
  def factsetById(id: String): FilePath =  factsets </> id
}

case class HdfsRepository(root: FilePath, run: ScoobiRun) extends Repository
case class LocalRepository(root: FilePath) extends Repository

/**
 * Repository on S3
 * all data is going to be stored on bucket/key
 * tmpDirectory is a transient directory (on Hdfl) that is used to import data and
 * convert them to the ivory format before pushing them to S3
 */
case class S3Repository(bucket: String, root: FilePath, tmp: FilePath, run: S3Run) extends Repository {
  val hdfs = HdfsRepository(tmp, run)
}

object Repository {
  val defaultS3TmpDirectory: FilePath =
    ".s3repository".toFilePath

  def fromUri(s: String, s3Run: S3Run, scoobiRun: ScoobiRun): String \/ Repository =
    Location.fromUri(s).map({
      case HdfsLocation(path) => HdfsRepository(path.toFilePath, scoobiRun)
      case LocalLocation(path) => LocalRepository(path.toFilePath)
      case S3Location(bucket, path) => S3Repository(bucket, path.toFilePath, defaultS3TmpDirectory, s3Run)
    })

  def fromHdfsPath(path: FilePath, run: ScoobiRun): HdfsRepository =
    HdfsRepository(path, run)

  def fromLocalPath(path: FilePath): LocalRepository =
    LocalRepository(path)

  def fromS3(bucket: String, path: FilePath, run: S3Run): S3Repository =
    S3Repository(bucket, path, defaultS3TmpDirectory, run)

  /** use a specific temporary directory to store ivory files before they are saved on S3 */
  def fromS3WithTemp(bucket: String, path: FilePath, tmp: FilePath, run: S3Run): S3Repository =
    S3Repository(bucket, path, tmp, run)
}
