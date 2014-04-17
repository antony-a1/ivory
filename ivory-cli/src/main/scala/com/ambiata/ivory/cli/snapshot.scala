package com.ambiata.ivory.cli

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.snapshot._
import com.ambiata.ivory.repository._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage._

import com.nicta.scoobi.Scoobi._

import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory
import org.joda.time.LocalDate
import java.util.Calendar

import scalaz.{DList => _, _}, Scalaz._

object snapshot extends ScoobiApp {

  case class CliArguments(repo: String, output: String, errors: String, date: LocalDate)

  val parser = new scopt.OptionParser[CliArguments]("snapshot") {
    head("""
         |Take a snapshot of facts from an ivory repo
         |
         |This will extract the latest facts for every entity relative to a date (default is now)
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")     action { (x, c) => c.copy(repo = x) }   required() text "Path to an ivory repository."
    opt[String]('o', "output")   action { (x, c) => c.copy(output = x) } required() text "Path to store snapshot."
    opt[String]('e', "errors")   action { (x, c) => c.copy(errors = x) } required() text "Path to store any errors."
    opt[Calendar]('d', "date")   action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text
      s"Optional date to take snapshot from, default is now."
  }

  def run {
    parser.parse(args, CliArguments("", "", "", LocalDate.now())).map(c => {
      val res = onHdfs(new Path(c.repo), new Path(c.output), new Path(c.errors), c.date)
      res.run(configuration).run.unsafePerformIO() match {
        case Ok(_)    => println(s"Successfully extracted snapshot from '${c.repo}' with date '${c.date}' and stored to '${c.output}'")
        case Error(e) => println(s"Failed! - ${e}")
      }
    })
  }

  def onHdfs(repo: Path, output: Path, errors: Path, date: LocalDate): ScoobiAction[(String, String)] =
    fatrepo.ExtractLatestWorkflow.onHdfs(repo, extractLatest(output, errors), date)

  def extractLatest(outputPath: Path, errorPath: Path)(repo: HdfsRepository, store: String, dictName: String, date: LocalDate): ScoobiAction[Unit] =
    HdfsSnapshot(repo.path, store, dictName, None, date, errorPath, EavtTextStorage.EavtTextStorerV1(outputPath.toString)).run
}
