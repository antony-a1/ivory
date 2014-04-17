package com.ambiata.ivory.cli

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.ingest.{DictionaryImporter, EavtTextImporter}
import com.ambiata.ivory.repository._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.alien.hdfs._

import com.nicta.scoobi.Scoobi._

import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory
import org.joda.time.DateTimeZone

import scalaz.{DList => _, _}, Scalaz._

object ingest extends ScoobiApp {

  val tombstone = List("☠")

  case class CliArguments(repo: String, dictionary: String, input: String, namespace: String, tmp: String, errors: String, timezone: DateTimeZone)

  val parser = new scopt.OptionParser[CliArguments]("ingest") {
    head("""
         |Fact ingestion pipeline.
         |
         |This will import a set of facts using the latest dictionary.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) }       required() text "Path to an ivory repository."
    opt[String]('e', "errors")     action { (x, c) => c.copy(errors = x) }     required() text "Path to store any errors."
    opt[String]('t', "tmp")        action { (x, c) => c.copy(tmp = x) }        required() text "Path to store tmp data."
    opt[String]('i', "input")      action { (x, c) => c.copy(input = x) }      required() text "Path to data to import."
    opt[String]('d', "dictionary")      action { (x, c) => c.copy(dictionary = x) }      required() text "Name of dictionary to use."
    opt[String]('n', "namespace")  action { (x, c) => c.copy(namespace = x) }  required() text "Namespace'."
    opt[String]('z', "timezone")        action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"

  }

  def run {
    parser.parse(args, CliArguments("", "", "", "", "", "", DateTimeZone.getDefault)).map(c => {
      val res = onHdfs(new Path(c.repo), c.dictionary, c.namespace, new Path(c.input), tombstone, new Path(c.tmp), new Path(c.errors), c.timezone)
      res.run(configuration).run.unsafePerformIO() match {
        case Ok(_)    => println(s"Successfully imported '${c.input}' into '${c.repo}'")
        case Error(e) => println(s"Failed! - ${e}")
      }
    })
  }

  def onHdfs(repo: Path, dictionary: String, namespace: String, input: Path, tombstone: List[String], tmp: Path, errors: Path, timezone: DateTimeZone): ScoobiAction[String] =
    fatrepo.ImportWorkflow.onHdfs(repo, defaultDictionaryImport(dictionary), importFeed(input, namespace), tombstone, tmp, errors, timezone)

  def defaultDictionaryImport(dictionary: String)(repo: HdfsRepository, name: String, tombstone: List[String], tmpPath: Path): Hdfs[Unit] =
    DictionaryImporter.onHdfs(repo.path, repo.dictionaryPath(dictionary), name)

  def importFeed(input: Path, namespace: String)(repo: HdfsRepository, factset: String, dname: String, tmpPath: Path, errorPath: Path, timezone: DateTimeZone): ScoobiAction[Unit] = for {
    dict <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dname))
    _    <- EavtTextImporter.onHdfs(repo, dict, factset, namespace, input, errorPath, timezone)
  } yield ()

}
