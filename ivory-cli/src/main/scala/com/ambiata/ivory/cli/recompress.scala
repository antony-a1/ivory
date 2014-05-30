package com.ambiata.ivory.cli

import com.ambiata.ivory.storage.legacy._
import com.nicta.scoobi.Scoobi._
import com.ambiata.mundane.io._

object recompress extends ScoobiApp {
  case class CliArguments(input: String, output: String, distribution: Int, dry: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("ivory-recompress") {
    head("""Clone an ivory repository, recompressing each part file.""")

    help("help") text "shows this usage text"

    opt[String]('i', "input")        action { (x, c) => c.copy(input = x) } required() text "Input ivory repository."

    opt[String]('o', "output")       action { (x, c) => c.copy(output = x) } required() text "Output ivory repository."

    opt[Unit]('d', "dry-run")       action { (_, c) => c.copy(dry = true) } required() text "Do a dry run only."

    opt[Unit]('n', "distribution")       action { (_, c) => c.copy(dry = true) } required() text "Number of mappers."
  }

  def run {
    parser.parse(args, CliArguments("", "", 20, false)).map { c =>
      Recompress.go(c.input, c.output, c.distribution, c.dry).run(configuration).run.unsafePerformIO.fold(
        ok => ok,
        err => { println(err); sys.exit(1) }
      )
    }
  }
}
