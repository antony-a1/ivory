package com.ambiata.ivory.cli

import scalaz.{DList => _, _}, Scalaz._
import com.ambiata.mundane.control._
import com.ambiata.ivory.validate._

object factDiff extends IvoryApp {

  case class CliArguments(input1: String, input2: String, output: String)

  val parser = new scopt.OptionParser[CliArguments]("fact-diff") {
    head("""
         |Compute diff between two sets of sequence files containing facts
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]("input1") action { (x, c) => c.copy(input1 = x) } required() text s"Hdfs glob path to the first facts."
    opt[String]("input2") action { (x, c) => c.copy(input2 = x) } required() text s"Hdfs glob path to the second facts."
    opt[String]('o', "output") action { (x, c) => c.copy(output = x) } required() text s"Hdfs location to store the difference."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", ""), ScoobiCmd { configuration => c =>
      val res = FactDiff.flatFacts(c.input1, c.input2, c.output)
      res.run(configuration).map {
        case _ => List(s"Any differences can be found in '${c.output}'")
      }
    })
}
