package com.ambiata.ivory.cli

import scalaz._, Scalaz._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.repository._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.saws.core.S3Action

object createRepository {

  lazy val configuration = new Configuration

  case class CliArguments(path: String = "", tmpDirectory: String = Repository.defaultS3TmpDirectory)

  val parser = new scopt.OptionParser[CliArguments]("create-repository"){
    head("""
|Create Ivory Repository.
|
|This app will create an empty ivory repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('p', "path") action { (x, c) => c.copy(path = x) } required() text
      s"Ivory repository to create. If the path starts with 's3://' we assume that this is a S3 repository"

    opt[String]('t', "temp-dir") action { (x, c) => c.copy(tmpDirectory = x) } optional() text
      s"Temporary directory path used to transfer data when interacting with S3. {user.home}/.s3repository by default"
  }

  def main(args: Array[String]) {
    parser.parse(args, CliArguments()).map { c =>
      val actions =
      if (c.path.startsWith("s3://")) {
        val repository = Repository.fromS3(new FilePath(c.path.replace("s3://", "")), new FilePath(c.tmpDirectory))
        CreateRepository.onS3(repository).eval
      }
      else
          CreateRepository.onHdfs(new Path(c.path)).run(configuration).run

      actions.unsafePerformIO() match {
        case Ok(v)    => println(s"Repository successfully created under ${c.path}.")
        case Error(e) => println(s"Failed to create repository: ${Result.asString(e)}")
      }
    }
  }
}
