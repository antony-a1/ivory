package com.ambiata.ivory.ingest

import scalaz._, Scalaz._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._

object DictionaryImporterCli {

  lazy val configuration = new Configuration

  case class CliArguments(repo: String, path: String, name: String)

  val parser = new scopt.OptionParser[CliArguments]("DictionaryImporterCli"){
    head("""
|Import dictionary into ivory.
|
|This app will parse the given dictionary and if valid, import it into the given repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) } required() text s"Hdfs path to the repository."
    opt[String]('p', "path") action { (x, c) => c.copy(path = x) } required() text s"Hdfs path to either a single dictionary file or directory of files to import."
    opt[String]('n', "name") action { (x, c) => c.copy(name = x) } required() text s"Name of the dictionary in the repository."
  }

  def main(args: Array[String]) {
    parser.parse(args, CliArguments("", "", "")).map(c => {
      DictionaryImporter.onHdfs(new Path(c.repo), new Path(c.path), c.name).run(configuration).run.unsafePerformIO() match {
        case Ok(v)    => println(s"Successfully imported dictionary ${c.path} into ${c.repo} under the name ${c.name}.")
        case Error(e) => sys.error(s"Failed to import dictionary: ${Result.asString(e)}")
      }
    })
  }
}
