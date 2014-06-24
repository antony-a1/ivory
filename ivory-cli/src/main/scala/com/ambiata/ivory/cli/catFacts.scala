package com.ambiata.ivory.cli

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._

import com.ambiata.ivory.extract.print.PrintFacts

object catFacts extends IvoryApp {
  case class CliArguments(delimiter: String = "|", tombstone: String = "NA", paths: List[String] = Nil)

  val parser = new scopt.OptionParser[CliArguments]("cat-facts") {
    head("""
           |Print facts as text (ENTITY-NAMESPACE-ATTRIBUTE-VALUE-DATETIME) to standard out, delimited by '|' or explicitly set delimiter.
           |The tombstone value is 'NA' by default.
           |""".stripMargin)

    help("help") text "shows this usage text"
    arg[String]("INPUT_PATH")       action { (x, c) => c.copy(paths = x :: c.paths) } required() unbounded() text
      "Glob path to snapshot facts sequence files or parent dir"
    opt[String]('d', "delimiter")   action { (x, c) => c.copy(delimiter = x) }        optional()             text
      "Delimiter (`|` by default)"
    opt[String]('t', "tombstone")   action { (x, c) => c.copy(tombstone = x) }        optional()             text
      "Tombstone (NA by default)"
  }

  val cmd = new IvoryCmd[CliArguments](parser, CliArguments(), HadoopCmd { conf => c =>
    PrintFacts.print(c.paths.map(new Path(_)), conf, c.delimiter, c.tombstone).executeT(consoleLogging).map(_ => Nil)
  })
}
