package com.ambiata.ivory.cli

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.ingest.EavtTextImporter
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory
import org.joda.time.DateTimeZone

import scalaz.{DList => _, _}, Scalaz._

object ingest extends IvoryApp {

  case class CliArguments(repo: String, input: String, namespace: String, timezone: DateTimeZone, runOnSingleMachine: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("ingest") {
    head("""
         |Fact ingestion pipeline.
         |
         |This will import a set of facts using the latest dictionary.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) }       required() text "Path to an ivory repository."
    opt[String]('i', "input")      action { (x, c) => c.copy(input = x) }      required() text "Path to data to import."
    opt[String]('n', "namespace")  action { (x, c) => c.copy(namespace = x) }  required() text "Namespace'."
    opt[String]('z', "timezone")        action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"
    opt[Unit]('s', "singularity")     action { (_, c) => c.copy(runOnSingleMachine = true) }   text "Avoid hadoop/scoobi and import directly."

  }

  def cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", "", DateTimeZone.getDefault, false), HadoopCmd { configuration => c =>
      val res = onHdfs(new Path(c.repo), c.namespace, new Path(c.input), c.timezone, c.runOnSingleMachine)
      res.run(configuration.modeIs(com.nicta.scoobi.core.Mode.Cluster)).map {
        case f => List(s"Successfully imported '${c.input}' as ${f} into '${c.repo}'")
      }
    })

  def onHdfs(repo: Path, namespace: String, input: Path, timezone: DateTimeZone, runOnSingleMachine: Boolean): ScoobiAction[Factset] =
    fatrepo.ImportWorkflow.onHdfs(repo, importFeed(input, namespace, runOnSingleMachine), timezone)

  def importFeed(input: Path, namespace: String, runOnSingleMachine: Boolean)(repo: HdfsRepository, factset: Factset, errorPath: Path, timezone: DateTimeZone): ScoobiAction[Unit] = for {
    dict <- ScoobiAction.fromResultTIO(IvoryStorage.dictionaryFromIvory(repo))
    conf <- ScoobiAction.scoobiConfiguration
    _    <- if (!runOnSingleMachine)
              EavtTextImporter.onHdfs(repo, dict, factset, namespace, input, errorPath, timezone, Some(new SnappyCodec))
            else
              ScoobiAction.fromResultTIO { EavtTextImporter.onHdfsDirect(conf, repo, dict, factset, namespace, input, errorPath, timezone, identity) }
  } yield ()

}
