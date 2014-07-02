package com.ambiata.ivory.cli

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.extract._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.apache.commons.logging.LogFactory
import org.joda.time.LocalDate
import java.util.Calendar
import java.util.UUID

import scalaz.{DList => _, _}, Scalaz._

object snapshot extends IvoryApp {

  case class CliArguments(repo: String, date: LocalDate, incremental: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("extract-snapshot") {
    head("""
         |Take a snapshot of facts from an ivory repo
         |
         |This will extract the latest facts for every entity relative to a date (default is now)
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")    action { (x, c) => c.copy(repo = x) }   required() text "Path to an ivory repository."
    opt[Unit]("no-incremental") action { (x, c) => c.copy(incremental = false) }   text "Flag to turn off incremental mode"
    opt[Calendar]('d', "date")  action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text
      s"Optional date to take snapshot from, default is now."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", LocalDate.now(), true), ScoobiCmd {
    configuration => c =>
      val runId = UUID.randomUUID
      val banner = s"""======================= snapshot =======================
                      |
                      |Arguments --
                      |
                      |  Run ID                  : ${runId}
                      |  Ivory Repository        : ${c.repo}
                      |  Extract At Date         : ${c.date.toString("yyyy/MM/dd")}
                      |  Incremental             : ${c.incremental}
                      |
                      |""".stripMargin
      println(banner)
      val res = HdfsSnapshot.takeSnapshot(new Path(c.repo), Date.fromLocalDate(c.date), c.incremental, Some(new SnappyCodec))
      res.run(configuration <| { c =>
        // MR1
        c.set("mapred.compress.map.output", "true")
        c.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec")

        // YARN
        c.set("mapreduce.map.output.compress", "true")
        c.set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")
      }).map {
        case (_, out) => List(banner, s"Output path: $out", "Status -- SUCCESS")
      }
  })

}
