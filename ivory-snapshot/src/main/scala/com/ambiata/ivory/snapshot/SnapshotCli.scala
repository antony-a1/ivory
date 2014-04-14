package com.ambiata.ivory.snapshot

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import java.util.Calendar
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._

object SnapshotCli extends ScoobiApp {

  case class CliArguments(repo: String, store: String, dictionary: String, entities: Option[String], date: LocalDate, output: String, errors: String, thrift: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("Snapshot") {
    head("""
         |Extract a snapshot from a given point in time.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repository")  action { (x, c) => c.copy(repo = x) }       required() text s"Hdfs location to an ivory repository."
    opt[String]('s', "store")       action { (x, c) => c.copy(store = x) }      required() text s"Feature Store name."
    opt[String]('d', "dictionary")  action { (x, c) => c.copy(dictionary = x) } required() text s"Feature Dictionary name."
    opt[String]('e', "entities")    action { (x, c) => c.copy(entities = Some(x)) }        text s"Optional Hdfs location to a file containing entity ids."
    opt[Calendar]('t', "date")      action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text s"Snapshot date."
    opt[String]('o', "output")      action { (x, c) => c.copy(output = x) }     required() text s"Hdfs location to store the snapshot."
    opt[String]("errors")           action { (x, c) => c.copy(errors = x) }     required() text s"Hdfs location to store validation errors."
    opt[Unit]("thrift")             action { (_, c) => c.copy(thrift = true) }             text s"Output facts in sequence files containing thrift serialized 'Entity' rows. This is the only format so far"
  }

  def run() {
    parser.parse(args, CliArguments("", "", "", None, LocalDate.now(), "", "", false)).map(c => {
      val res = Snapshot.onHdfs(new Path(c.repo), c.store, c.dictionary, c.entities.map(new Path(_)), c.date, new Path(c.output), new Path(c.errors)).run(configuration).run.unsafePerformIO()
      res match {
        case Ok(_)    => println(s"successfully wrote snapshot to ${c.output}")
        case Error(e) => sys.error(s"failed! ${e}")
      }
    })
  }
}
