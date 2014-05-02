package com.ambiata.ivory.core

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io.FilePath
-
sealed trait Location
case class HdfsLocation(path: String) extends Location
case class S3Location(bucket: String, path: String) extends Location
case class LocalLocation(path: String) extends Location

object Location {
  def fromUri(s: String): String \/ Location = try {
    val uri = new java.net.URI(s)
    uri.getScheme match {
      case "hdfs" =>
        HdfsLocation(uri.getPath).right
      case "s3" =>
        S3Location(uri.getHost, uri.getPath.drop(1)).right
      case "file" =>
        LocalLocation(uri.toURL.getFile).right
      case null =>
        LocalLocation(uri.getPath).right
      case _ =>
        s"Unknown or invalid repository scheme [${uri.getScheme}]".left
    }
  } catch {
    case e: java.net.URISyntaxException =>
      e.getMessage.left
  }
}
