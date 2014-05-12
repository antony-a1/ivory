package com.ambiata.ivory.cli

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.extract._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._

import com.nicta.scoobi.Scoobi._

import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory
import org.joda.time.LocalDate
import java.util.Calendar
import java.util.UUID

import scalaz.{DList => _, _}, Scalaz._

object snapshot extends ScoobiApp {

  case class CliArguments(repo: String, output: String, date: LocalDate, incremental: Option[String])

  val parser = new scopt.OptionParser[CliArguments]("snapshot") {
    head("""
         |Take a snapshot of facts from an ivory repo
         |
         |This will extract the latest facts for every entity relative to a date (default is now)
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")        action { (x, c) => c.copy(repo = x) }   required() text "Path to an ivory repository."
    opt[String]('o', "output")      action { (x, c) => c.copy(output = x) } required() text "Path to store snapshot."
    opt[String]('i', "incremental") action { (x, c) => c.copy(incremental = Some(x)) } text "Path to a snapshot to add in. Path must contain a .snapmeta file."
    opt[Calendar]('d', "date")      action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text
      s"Optional date to take snapshot from, default is now."
  }

  def run {
    val runId = UUID.randomUUID
    parser.parse(args, CliArguments("", "", LocalDate.now(), None)).map(c => {
      val errors = s"${c.repo}/errors/snapshot/${runId}"
      val banner = s"""======================= snapshot =======================
                      |
                      |Arguments --
                      |
                      |  Run ID                  : ${runId}
                      |  Ivory Repository        : ${c.repo}
                      |  Extract At Date         : ${c.date.toString("yyyy/MM/dd")}
                      |  Output Path             : ${c.output}
                      |  Errors                  : ${errors}
                      |  Incremental             : ${c.incremental.getOrElse("")}
                      |
                      |""".stripMargin
      println(banner)
      val res = HdfsSnapshot.takeSnapshot(new Path(c.repo), new Path(c.output), new Path(errors), c.date, c.incremental.map(i => new Path(i)))
      res.run(configuration <| { c =>
        // MR1
        c.set("mapred.compress.map.output", "true")
        c.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec")

        // YARN
        c.set("mapreduce.map.output.compress", "true")
        c.set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")
      }).run.unsafePerformIO() match {
        case Ok(_) =>
          println(banner)
          println("Status -- SUCCESS")
        case Error(e) =>
          println(s"Failed! - ${e}")
      }
    })
  }

}
