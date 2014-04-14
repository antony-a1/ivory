package com.ambiata.ivory.validate

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._

object ValidateFactSetCli extends ScoobiApp {

  case class CliArguments(repo: String, dictionary: String, factset: String, output: String)

  val parser = new scopt.OptionParser[CliArguments]("ValidateFactSet") {
    head("""
         |Validate a fact set.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repository")  action { (x, c) => c.copy(repo = x) }       required() text s"Hdfs location to an ivory repository."
    opt[String]('d', "dictionary")  action { (x, c) => c.copy(dictionary = x) } required() text s"Feature Dictionary name."
    opt[String]('f', "factset")     action { (x, c) => c.copy(factset = x) }    required() text s"Fact Set name."
    opt[String]('o', "output")      action { (x, c) => c.copy(output = x) }     required() text s"Hdfs location to store validation errors."
  }

  def run() {
    parser.parse(args, CliArguments("", "", "", "")).map(c => {
      val res = Validate.validateHdfsFactSet(new Path(c.repo), c.factset, c.dictionary, new Path(c.output)).run(configuration).run.unsafePerformIO()
      res match {
        case Ok(_)    => println(s"validated fact set ${c.factset} with dictionary ${c.dictionary} in the ${c.repo} repository.")
        case Error(e) => sys.error(s"failed! ${e}")
      }
    })
  }
}
