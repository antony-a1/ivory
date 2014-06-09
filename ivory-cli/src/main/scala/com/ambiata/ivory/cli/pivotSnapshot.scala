package com.ambiata.ivory.cli

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.extract._

import com.nicta.scoobi.Scoobi._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.apache.commons.logging.LogFactory

import org.joda.time.LocalDate
import java.util.Calendar

import scalaz.{DList => _, _}, Scalaz._

object pivotSnapshot extends ScoobiApp {

  case class CliArguments(repo: String, output: String, errors: String, delim: Char, tombstone: String, date: LocalDate)

  implicit val charRead: scopt.Read[Char] =
    scopt.Read.reads(str => {
      val chars = str.toCharArray
      chars.length match {
        case 0 => throw new IllegalArgumentException(s"'${str}' can not be empty!")
        case 1 => chars(0)
        case l => throw new IllegalArgumentException(s"'${str}' is not a char!")
      }
    })

  val parser = new scopt.OptionParser[CliArguments]("extract-pivot-snapshot") {
    head("""
         |Pivot ivory data using DenseRowTextStorageV1.DenseRowTextStorer
         |
         |This will read partitioned data using PartitionFactThriftStorageV2 and store as row oriented text.
         |A .dictionary file will be stored containing the fields
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")   action { (x, c) => c.copy(repo = x) }       required() text "Path to ivory repository."
    opt[String]('o', "output") action { (x, c) => c.copy(output = x) }     required() text "Path to store pivot data."
    opt[String]('e', "errors") action { (x, c) => c.copy(errors = x) }     required() text "Path to store errors."
    opt[String]("tombstone")   action { (x, c) => c.copy(tombstone = x) }             text "Output value to use for missing data, default is 'NA'"
    opt[Char]("delim")         action { (x, c) => c.copy(delim = x) }                 text "Output delimiter, default is '|'"
    opt[Calendar]("date")      action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text
      s"Optional date to take snapshot from, default is now."

  }

  def run {
    parser.parse(args, CliArguments("", "", "", '|', "NA", new LocalDate)).map(c => {
      val banner = s"""======================= pivot =======================
                      |
                      |Arguments --
                      |
                      |  Repo Path               : ${c.repo}
                      |  Output Path             : ${c.output}
                      |  Errors Path             : ${c.errors}
                      |  Delim                   : ${c.delim}
                      |  Tombstone               : ${c.tombstone}
                      |  Snapshot Date           : ${c.date.toString("yyyy-MM-dd")}
                      |
                      |""".stripMargin
      println(banner)
      val res = Pivot.onHdfsFromSnapshot(new Path(c.repo), new Path(c.output), new Path(c.errors), c.delim, c.tombstone, Date.fromLocalDate(c.date), Some(new SnappyCodec))
      res.run(configuration).run.unsafePerformIO() match {
        case Ok(_) =>
          println(banner)
          println("Status -- SUCCESS")
        case Error(e) =>
          println(s"Failed! - ${e}")
      }
    })
  }
}
