package com.ambiata.ivory.cli

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.validate._

object validateFactSet extends IvoryApp {

  case class CliArguments(repo: String, factset: Factset, output: String)

  val parser = new scopt.OptionParser[CliArguments]("validate-fact-set") {
    head("""
         |Validate a fact set.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repository")  action { (x, c) => c.copy(repo = x) }       required() text s"Hdfs location to an ivory repository."
    opt[String]('f', "factset")     action { (x, c) => c.copy(factset = Factset(x)) }    required() text s"Fact Set name."
    opt[String]('o', "output")      action { (x, c) => c.copy(output = x) }     required() text s"Hdfs location to store validation errors."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", Factset(""), ""), ScoobiCmd { configuration => c =>
    Validate.validateHdfsFactSet(new Path(c.repo), c.factset, new Path(c.output)).run(configuration).map {
      case _ => List(s"validated fact set ${c.factset} in the ${c.repo} repository.")
    }
  })
}
