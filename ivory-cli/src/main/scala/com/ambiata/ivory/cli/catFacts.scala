package com.ambiata.ivory.cli

import com.nicta.scoobi.Scoobi._
import com.ambiata.mundane.io._
import com.ambiata.ivory.extract.print.PrintFacts

object catFacts extends App {
  case class CliArguments(delimiter: String = "|", tombstone: String = "NA", path: String = "")

  val parser = new scopt.OptionParser[CliArguments]("cat-facts") {
    head("""
           |Print facts as text (ENTITY-NAMESPACE-ATTRIBUTE-VALUE-DATETIME) to standard out, delimited by '|' or explicitly set delimiter.
           |The tombstone value is 'NA' by default.
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('p', "path")        action { (x, c) => c.copy(path = x) }      required() text "Input path (glob path to fact sequence file)"
    opt[String]('d', "delimiter")   action { (x, c) => c.copy(delimiter = x) } optional() text "Delimiter (`|` by default)"
    opt[String]('t', "tombstone")   action { (x, c) => c.copy(tombstone = x) } optional() text "Tombstone (NA by default)"
  }

  parser.parse(args, CliArguments()).map { c =>
    PrintFacts.printGlob(c.path, "*out*", c.delimiter, c.tombstone).execute(consoleLogging).unsafePerformIO.fold(
      ok  => ok,
      err => println(err)
    )
  }
}
