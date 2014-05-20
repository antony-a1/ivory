package com.ambiata.ivory.cli

import com.nicta.scoobi.Scoobi._
import com.ambiata.mundane.io._
import com.ambiata.ivory.extract.print.PrintErrors

object catErrors extends App {
  case class CliArguments(delimiter: String = "|", path: String = "")

  val parser = new scopt.OptionParser[CliArguments]("ivory-cat-errors") {
    head("""
           |ivory-cat-errors [-d|--delimiter] GLOB_PATH_TO_ERROR_SEQUENCE_FILE
           |Print errors as text (LINE-MESSAGE) to standard out, delimited by '|' or explicitly set delimiter.
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('p', "path")        action { (x, c) => c.copy(path = x) }      required() text "Input path (glob path to error sequence file)"
    opt[String]('d', "delimiter")   action { (x, c) => c.copy(delimiter = x) } optional() text "Delimiter (`|` by default)"
  }

  parser.parse(args, CliArguments()).map { c =>
    PrintErrors.printGlob(c.path, "*out*", c.delimiter).execute(consoleLogging).unsafePerformIO.fold(
      ok  => ok,
      err => println(err)
    )
  }
}
