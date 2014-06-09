package com.ambiata.ivory.validate

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._

object validateStore extends ScoobiApp {
  case class CliArguments(repo: String, store: String, dictionary: String, output: String, includeOverridden: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("validate-store") {
    head("""
         |Validate a feature store.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repository")  action { (x, c) => c.copy(repo = x) }       required() text s"Hdfs location to an ivory repository."
    opt[String]('s', "store")       action { (x, c) => c.copy(store = x) }      required() text s"Feature Store name."
    opt[String]('d', "dictionary")  action { (x, c) => c.copy(dictionary = x) } required() text s"Feature Dictionary name."
    opt[String]('o', "output")      action { (x, c) => c.copy(output = x) }     required() text s"Hdfs location to store validation errors."
    opt[Unit]("include-overridden") action { (_, c) => c.copy(includeOverridden = true) }  text s"Validate overridden facts. Default is false."
  }

  def run() {
    parser.parse(args, CliArguments("", "", "", "", false)).map(c => {
      val res = Validate.validateHdfsStore(new Path(c.repo), c.store, c.dictionary, new Path(c.output), c.includeOverridden).run(configuration).run.unsafePerformIO()
      res match {
        case Ok(_)    => println(s"validated feature store ${c.store} with dictionary ${c.dictionary} in the ${c.repo} repository.")
        case Error(e) => sys.error(s"failed! ${e}")
      }
    })
  }
}
