package com.ambiata.ivory.chord

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._

object ChordCli extends ScoobiApp {

  case class CliArguments(repo: String, store: String, dictionary: String, entities: String, output: String, errors: String)

  val parser = new scopt.OptionParser[CliArguments]("Chord") {
    head("""
         |Extract the latest facts given a list of entity/date pairs.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repository")  action { (x, c) => c.copy(repo = x) }       required() text s"Hdfs location to an ivory repository."
    opt[String]('s', "store")       action { (x, c) => c.copy(store = x) }      required() text s"Feature Store name."
    opt[String]('d', "dictionary")  action { (x, c) => c.copy(dictionary = x) } required() text s"Feature Dictionary name."
    opt[String]('e', "entities")    action { (x, c) => c.copy(entities = x) }   required() text s"Hdfs location to a file containing entity/date pairs."
    opt[String]('o', "output")      action { (x, c) => c.copy(output = x) }     required() text s"Hdfs location to store the snapshot."
    opt[String]("errors")           action { (x, c) => c.copy(errors = x) }     required() text s"Hdfs location to store validation errors."
  }

  def run() {
    parser.parse(args, CliArguments("", "", "", "", "", "")).map(c => {
      val res = Chord.onHdfs(new Path(c.repo), c.store, c.dictionary, new Path(c.entities), new Path(c.output), new Path(c.errors)).run(configuration).run.unsafePerformIO()
      res match {
        case Ok(_)    => println(s"successfully wrote output to ${c.output}")
        case Error(e) => sys.error(s"failed! ${e}")
      }
    })
  }
}

