package com.ambiata.ivory.example.fatrepo

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import org.joda.time.DateTimeZone


object ImportWorkflowCli extends ScoobiApp {

  val tombstone = List("☠")

  case class CliArguments(repo: String, dictionary: String, input: String, namespace: String, tmp: String, errors: String, timezone: DateTimeZone)

  val parser = new scopt.OptionParser[CliArguments]("ImportWorkflow") {
    head("""
         |Example workflow to import dictionaries and features into an ivory repository and create a new feature store.
         |
         |This workflow will do the following:
         |1. Create a new repository if one doesn't exist.
         |2. Import the given dictionary, adding to any existing dictionaries and overriding ones with the same name.
         |3. Create an empty fact set ready to import data into.
         |4. Import the features into the fact set.
         |5. Create a feature store with the fact set and any previous fact sets in it.
         |
         |Example:
         |hadoop jar ivory.jar com.ambiata.ivory.example.fatrepo.ImportWorkflowCli -r ivory_repo -d dictionaries -e errors -i input -z Australia/Sydney
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) }       required() text "Path to an ivory repository."
    opt[String]('d', "dictionary") action { (x, c) => c.copy(dictionary = x) } required() text "Path to a set of dictionaries to import."
    opt[String]('e', "errors")     action { (x, c) => c.copy(errors = x) }     required() text "Path to store any errors."
    opt[String]('t', "tmp")        action { (x, c) => c.copy(tmp = x) }        required() text "Path to store tmp data."
    opt[String]('i', "input")      action { (x, c) => c.copy(input = x) }      required() text "Path to data to import."
    opt[String]('n', "namespace")  action { (x, c) => c.copy(namespace = x) }             text "Namespace, default 'namespace'."
    opt[String]('z', "timezone")        action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"

  }

  def run {
    parser.parse(args, CliArguments("", "", "", "namespace", "", "", DateTimeZone.getDefault)).map(c => {
      val res = ImportWorkflow.onHdfs(new Path(c.repo), new Path(c.dictionary), c.namespace, new Path(c.input), tombstone, new Path(c.tmp), new Path(c.errors), c.timezone)
      res.run(configuration).run.unsafePerformIO() match {
        case Ok(_)    => println(s"Successfully imported '${c.input}' into '${c.repo}'")
        case Error(e) => println(s"Failed! - ${e}")
      }
    })
  }
}
