package com.ambiata.ivory.cli

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.validate._

object validateStore extends IvoryApp {
  case class CliArguments(repo: String, store: String, output: String, includeOverridden: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("validate-store") {
    head("""
         |Validate a feature store.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repository")  action { (x, c) => c.copy(repo = x) }       required() text s"Hdfs location to an ivory repository."
    opt[String]('s', "store")       action { (x, c) => c.copy(store = x) }      required() text s"Feature Store name."
    opt[String]('o', "output")      action { (x, c) => c.copy(output = x) }     required() text s"Hdfs location to store validation errors."
    opt[Unit]("include-overridden") action { (_, c) => c.copy(includeOverridden = true) }  text s"Validate overridden facts. Default is false."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", "", false), ScoobiCmd { configuration => c =>
      Validate.validateHdfsStore(new Path(c.repo), c.store, new Path(c.output), c.includeOverridden).run(configuration).map {
        case _ => List(s"validated feature store ${c.store} in the ${c.repo} repository.")
      }
    })
}
