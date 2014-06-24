package com.ambiata.ivory.cli

import com.ambiata.ivory.storage.legacy._
import com.ambiata.mundane.io._

object recompress extends IvoryApp {
  case class CliArguments(input: String, output: String, distribution: Int, dry: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("recompress") {
    head("""Clone an ivory repository, recompressing each part file.""")

    help("help") text "shows this usage text"

    opt[String]('i', "input")        action { (x, c) => c.copy(input = x) } required() text "Input ivory repository."

    opt[String]('o', "output")       action { (x, c) => c.copy(output = x) } required() text "Output ivory repository."

    opt[Unit]('d', "dry-run")       action { (_, c) => c.copy(dry = true) } optional() text "Do a dry run only."

    opt[Int]('n', "distribution")       action { (x, c) => c.copy(distribution = x) } optional() text "Number of mappers."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", 20, false), ScoobiCmd { configuration => c =>
    Recompress.go(c.input, c.output, c.distribution, c.dry).run(configuration).map(_ => Nil)
  })
}
