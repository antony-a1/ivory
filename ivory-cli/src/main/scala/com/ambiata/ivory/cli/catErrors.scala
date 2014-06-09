package com.ambiata.ivory.cli

import com.ambiata.ivory.extract.print.PrintErrors

object catErrors extends IvoryApp {
  case class CliArguments(delimiter: String = "|", path: String = "")

  val parser = new scopt.OptionParser[CliArguments]("cat-errors") {
    head("""
           |Print errors as text (LINE-MESSAGE) to standard out, delimited by '|' or explicitly set delimiter.
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('p', "path")        action { (x, c) => c.copy(path = x) }      required() text "Input path (glob path to error sequence file)"
    opt[String]('d', "delimiter")   action { (x, c) => c.copy(delimiter = x) } optional() text "Delimiter (`|` by default)"
  }

  val cmd = new IvoryCmd[CliArguments](parser, CliArguments(), ActionCmd { c =>
    PrintErrors.printGlob(c.path, "*out*", c.delimiter)
  })
}
