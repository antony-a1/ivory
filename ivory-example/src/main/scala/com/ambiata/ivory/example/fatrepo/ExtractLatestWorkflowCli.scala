package com.ambiata.ivory.example.fatrepo

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import java.util.Calendar
import com.ambiata.mundane.control._

import com.ambiata.ivory.core.Date

object ExtractLatestWorkflowCli extends ScoobiApp {

  case class CliArguments(repo: String, output: String, errors: String, date: LocalDate)

  val parser = new scopt.OptionParser[CliArguments]("ImportWorkflow") {
    head("""
         |Example workflow to import into a fatrepo (one which contains all facts over all of time)
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")   action { (x, c) => c.copy(repo = x) }   required() text s"Repository dir"
    opt[String]('o', "output") action { (x, c) => c.copy(output = x) } required() text s"Output dir"
    opt[String]('e', "errors") action { (x, c) => c.copy(errors = x) } required() text s"Error dir"
    opt[Calendar]('d', "date") action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text s"Namespace for facts, default is 'namespace'."
  }

  def run {
    parser.parse(args, CliArguments("", "", "", LocalDate.now())).map(c => {
      val res = ExtractLatestWorkflow.onHdfs(new Path(c.repo), new Path(c.output), new Path(c.errors), Date.fromLocalDate(c.date))
      res.run(configuration).run.unsafePerformIO() match {
        case Ok(_)    => println(s"Successfully extracted latest facts at '${c.date.toString("yyyy-MM-dd")}' from '${c.repo}' to '${c.output}'")
        case Error(e) => println(s"Failed! - ${e}")
      }
    })
  }
}
