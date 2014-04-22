package com.ambiata.ivory.core

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io.FilePath

sealed trait Repository

case class HdfsRepository(path: Path) extends Repository {
  lazy val errorsPath = new Path(path, "errors")
  lazy val metadataPath = new Path(path, "metadata")
  lazy val factsetsPath = new Path(path, "factsets")

  lazy val dictionariesPath = new Path(metadataPath, "dictionaries")
  lazy val storesPath = new Path(metadataPath, "stores")

  def dictionaryPath(name: String): Path =
    new Path(dictionariesPath, name)

  def storePath(name: String): Path =
    new Path(storesPath, name)

  def factsetPath(name: String): Path =
    new Path(factsetsPath, name)
}

case class LocalRepository(path: String) extends Repository {
  lazy val errorsPath = s"$path/errors"
  lazy val metadataPath = s"$path/metadata"
  lazy val factsetsPath = s"$path/factsets"

  lazy val dictionariesPath = s"$metadataPath/dictionaries"
  lazy val storesPath = s"$metadataPath/stores"

  def dictionaryPath(name: String): String =
    s"$dictionariesPath/$name"

  def storePath(name: String): String =
    s"$storesPath/$name"

  def factsetPath(name: String): String =
    s"$factsetsPath/$name"
}

/**
 * Repository on S3
 * all data is going to be stored on bucket/key
 * tmpDirectory is a transient directory (on Hdfl) that is used to import data and
 * convert them to the ivory format before pushing them to S3
 */
case class S3Repository(bucket: String, key: String, tmpDirectory: String = ".s3Repository") extends Repository {
  lazy val hdfsRepository = HdfsRepository(new Path(tmpDirectory))

  lazy val repositoryDir = if (key.endsWith("/")) key else key+"/"

  lazy val metadata    = s"${repositoryDir}metadata/"

  lazy val factsets    = "factsets"
  lazy val factsetsKey = s"${repositoryDir}$factsets/"

  lazy val dictionaries    = "dictionaries"
  lazy val dictionariesKey = s"${metadata}$dictionaries/"

  lazy val stores    = "stores/"
  lazy val storesKey = s"${metadata}$stores"

  def dictionaryKey(name: String): String =
    s"${dictionariesKey}$name"

  def storeKey(name: String): String =
    s"${storesKey}$name"

  def factsetKey(name: String): String =
    s"${factsetsKey}$name"
}

object Repository {
  def fromUri(s: String): String \/ Repository = try {
    val uri = new java.net.URI(s)
    uri.getScheme match {
      case "hdfs" =>
        HdfsRepository(new Path(uri.getPath)).right
      case "s3" =>
        S3Repository(uri.getHost, uri.getPath.drop(1)).right
      case "file" =>
        LocalRepository(uri.toURL.getFile).right
      case null =>
        LocalRepository(uri.getPath).right
      case _ =>
        s"Unknown or invalid repository scheme [${uri.getScheme}]".left
    }
  } catch {
    case e: java.net.URISyntaxException =>
      e.getMessage.left
  }


  def fromHdfsPath(path: Path): HdfsRepository =
    HdfsRepository(path)

  def fromLocalPath(path: FilePath): LocalRepository =
    LocalRepository(path.path)

  def fromS3(path: FilePath): S3Repository =
    S3Repository(path.rootname.path, path.fromRoot.path)

  /** use a specific temporary directory to store ivory files before they are saved on S3 */
  def fromS3(path: FilePath, tmpDir: FilePath): S3Repository =
    S3Repository(path.rootname.path, path.fromRoot.path, tmpDir.path)

  val defaultS3TmpDirectory = ".s3repository"
}
