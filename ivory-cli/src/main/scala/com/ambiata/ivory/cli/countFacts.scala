package com.ambiata.ivory.cli

import org.apache.hadoop.fs.Path
import com.ambiata.ivory.extract.FactCount

object countFacts extends IvoryApp {
  case class CliArguments(path: String)

  val parser = new scopt.OptionParser[CliArguments]("count-facts") {
    head("""
           | Count the number of facts in a snapshot
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('p', "path") action { (x, c) => c.copy(path = x) }      required() text "Input path (glob path to fact sequence file)"
  }

  val cmd = new IvoryCmd[CliArguments](parser, CliArguments(""), ScoobiCmd { configuration => c =>
    FactCount.flatFacts(new Path(c.path, "*out*")).run(configuration).map(count =>
      List(s"Fact count: $count", "Status -- SUCCESS"))
  })
}
