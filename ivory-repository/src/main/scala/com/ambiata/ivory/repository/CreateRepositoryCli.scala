package com.ambiata.ivory.repository

import scalaz._, Scalaz._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._

object CreateRepositoryCli {

  lazy val configuration = new Configuration

  case class CliArguments(path: String)

  val parser = new scopt.OptionParser[CliArguments]("CreateRepositoryCli"){
    head("""
|Create Ivory Repository.
|
|This app will create an empty ivory repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('p', "path") action { (x, c) => c.copy(path = x) } required() text s"Hdfs path to create repository."
  }

  def main(args: Array[String]) {
    parser.parse(args, CliArguments("")).map(c =>
      CreateRepository.onHdfs(new Path(c.path)).run(configuration).run.unsafePerformIO() match {
        case Ok(v)    => println(s"Repository successfully created under ${c.path}.")
        case Error(e) => println(s"Failed to create repository: ${Result.asString(e)}")
      }
    )
  }
}
