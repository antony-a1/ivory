package com.ambiata.ivory.cli

import scalaz._, Scalaz._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.saws.core.S3Action

import com.nicta.scoobi.Scoobi._

object createRepository extends IvoryApp {

  case class CliArguments(path: String = "", tmpDirectory: FilePath = Repository.defaultS3TmpDirectory)

  val parser = new scopt.OptionParser[CliArguments]("create-repository"){
    head("""
|Create Ivory Repository.
|
|This app will create an empty ivory repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('p', "path") action { (x, c) => c.copy(path = x) } required() text
      s"Ivory repository to create. If the path starts with 's3://' we assume that this is a S3 repository"

    opt[String]('t', "temp-dir") action { (x, c) => c.copy(tmpDirectory = x.toFilePath) } optional() text
      s"Temporary directory path used to transfer data when interacting with S3. {user.home}/.s3repository by default"
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments(), HadoopCmd { configuration => c =>
      println("Created configuration: " + configuration)

      val actions =
      if (c.path.startsWith("s3://")) {
        val p = c.path.replace("s3://", "").toFilePath
        val repository = Repository.fromS3WithTemp(p.rootname.path, p.fromRoot, c.tmpDirectory, configuration)
        CreateRepository.onS3(repository).evalT
      }
      else
          CreateRepository.onHdfs(new Path(c.path)).run(configuration)

      actions.map {
        case _ => List(s"Repository successfully created under ${c.path}.")
      }
  })
}
