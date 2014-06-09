package com.ambiata.ivory.cli

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import com.ambiata.mundane.control._
import com.ambiata.ivory.validate._

object factDiff extends ScoobiApp {

  case class CliArguments(input1: String, input2: String, output: String, errors: String)

  val parser = new scopt.OptionParser[CliArguments]("fact-diff") {
    head("""
         |Compute diff between two files containing facts
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]("input1") action { (x, c) => c.copy(input1 = x) } required() text s"Hdfs location to the first facts file."
    opt[String]("input2") action { (x, c) => c.copy(input2 = x) } required() text s"Hdfs location to the first facts file."
    opt[String]('o', "output") action { (x, c) => c.copy(output = x) } required() text s"Hdfs location to store the difference."
    opt[String]('e', "errors") action { (x, c) => c.copy(errors = x) } required() text s"Hdfs location to store any errors."
  }

  def run() {
    parser.parse(args, CliArguments("", "", "", "")).map(c => {
      val res = FactDiff.scoobiJob(c.input1, c.input2, c.output, c.errors)
      res.run(configuration).run.unsafePerformIO() match {
        case Ok(_)    => println(s"Any differences can be found in '${c.output}', errors in '${c.errors}'")
        case Error(e) => println(s"Failed with '${e}'")
      }
    })
  }
}
