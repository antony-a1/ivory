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
import java.util.UUID

import scalaz.{DList => _, _}, Scalaz._

object snapshot extends ScoobiApp {

  case class CliArguments(repo: String, output: String, date: LocalDate, storer: SnapStorer, incremental: Option[(String, String)])

  implicit val snapStorerRead: scopt.Read[SnapStorer] =
  scopt.Read.reads(str => str match {
    case "eavttext"     => EavtTextSnapStorer
    case "denserowtext" => DenseRowTextSnapStorer
    case s              => throw new IllegalArgumentException(s"Storer '${s}' not found!")
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
    opt[(String, String)]('i', "incremental")   action { (x, c) => c.copy(incremental = Some(x)) } required() text "Path to of a snapshot to add in, and the version of the store to diff against."
    opt[Calendar]('d', "date")     action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text
      s"Optional date to take snapshot from, default is now."
    opt[SnapStorer]('s', "storer") action { (x, c) => c.copy(storer = x) }            text "Name of storer to use 'eavttext', or 'denserowtext'"
  }

  def run {
    val runId = UUID.randomUUID
    parser.parse(args, CliArguments("", "", LocalDate.now(), EavtTextSnapStorer, None)).map(c => {
      val errors = s"${c.repo}/errors/snapshot/${runId}"
      val banner = s"""======================= snapshot =======================
                      |
                      |Arguments --
                      |
                      |  Run ID                  : ${runId}
                      |  Ivory Repository        : ${c.repo}
                      |  Extract At Date         : ${c.date.toString("yyyy/MM/dd")}
                      |  Output Format           : ${c.storer.toString}
                      |  Output Path             : ${c.output}
                      |  Errors                  : ${errors}
                      |
                      |""".stripMargin
      println(banner)
      val res = onHdfs(new Path(c.repo), new Path(c.output), new Path(errors), c.date, c.storer, c.incremental)
      res.run(configuration).run.unsafePerformIO() match {
        case Ok(_) =>
          println(banner)
          println("Status -- SUCCESS")
        case Error(e) =>
          println(s"Failed! - ${e}")
      }
    })
  }

  def onHdfs(repo: Path, output: Path, errors: Path, date: LocalDate, storer: SnapStorer, incremental: Option[(String, String)]): ScoobiAction[(String, String)] =
    fatrepo.ExtractLatestWorkflow.onHdfs(repo, extractLatest(output, errors, storer, incremental), date)

  def extractLatest(outputPath: Path, errorPath: Path, storer: SnapStorer, incremental: Option[(String, String)])(repo: HdfsRepository, store: String, dictName: String, date: LocalDate): ScoobiAction[Unit] = for {
    d  <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dictName))
    s   = storer match {
      case DenseRowTextSnapStorer => DenseRowTextStorageV1.DenseRowTextStorer(outputPath.toString + "/dense", d)
      case EavtTextSnapStorer     => EavtTextStorageV1.EavtTextStorer(outputPath.toString + "/eavt")
    }
    _  <- HdfsSnapshot(repo.path, store, dictName, None, date, outputPath, errorPath, s, incremental).run
  } yield ()
}

sealed trait SnapStorer
case object DenseRowTextSnapStorer extends SnapStorer {
  override def toString = "denserowtext"
}
case object EavtTextSnapStorer extends SnapStorer {
  override def toString = "eavttext"
}
