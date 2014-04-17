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

  case class CliArguments(repo: String, output: String, errors: String, date: LocalDate, storer: SnapStorer)

  implicit val snapStorerRead: scopt.Read[SnapStorer] =
  scopt.Read.reads(str => str match {
    case "eavttext"     => EavtText
    case "denserowtext" => DenseRowText
  })

  val parser = new scopt.OptionParser[CliArguments]("snapshot") {
    head("""
         |Take a snapshot of facts from an ivory repo
         |
         |This will extract the latest facts for every entity relative to a date (default is now)
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")       action { (x, c) => c.copy(repo = x) }   required() text "Path to an ivory repository."
    opt[String]('o', "output")     action { (x, c) => c.copy(output = x) } required() text "Path to store snapshot."
    opt[String]('e', "errors")     action { (x, c) => c.copy(errors = x) } required() text "Path to store any errors."
    opt[Calendar]('d', "date")     action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text
      s"Optional date to take snapshot from, default is now."
    opt[SnapStorer]('s', "storer") action { (x, c) => c.copy(storer = x) }            text "Name of storer to use 'eavttext', or 'denserowtext'"
  }

  def run {
    parser.parse(args, CliArguments("", "", "", LocalDate.now(), EavtText)).map(c => {
      val res = onHdfs(new Path(c.repo), new Path(c.output), new Path(c.errors), c.date, c.storer)
      res.run(configuration).run.unsafePerformIO() match {
        case Ok(_)    => println(s"Successfully extracted snapshot from '${c.repo}' with date '${c.date}' and stored to '${c.output}'")
        case Error(e) => println(s"Failed! - ${e}")
      }
    })
  }

  def onHdfs(repo: Path, output: Path, errors: Path, date: LocalDate, storer: SnapStorer): ScoobiAction[(String, String)] =
    fatrepo.ExtractLatestWorkflow.onHdfs(repo, extractLatest(output, errors, storer), date)

  def extractLatest(outputPath: Path, errorPath: Path, storer: SnapStorer)(repo: HdfsRepository, store: String, dictName: String, date: LocalDate): ScoobiAction[Unit] = for {
    d  <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dictName))
    s   = storer match {
      case DenseRowText => DenseRowTextStorage.DenseRowTextStorer(outputPath.toString, d)
      case EavtText     => EavtTextStorage.EavtTextStorerV1(outputPath.toString)
    }
    _  <- HdfsSnapshot(repo.path, store, dictName, None, date, errorPath, s).run
  } yield ()
}

sealed trait SnapStorer
case object DenseRowText extends SnapStorer
case object EavtText extends SnapStorer
