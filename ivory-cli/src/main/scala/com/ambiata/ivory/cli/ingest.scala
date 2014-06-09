package com.ambiata.ivory.cli

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.ingest.{DictionaryImporter, EavtTextImporter}
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

  val tombstone = List("☠")

  case class CliArguments(repo: String, dictionary: Option[String], input: String, namespace: String, tmp: String, timezone: DateTimeZone, runOnSingleMachine: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("ingest") {
    head("""
         |Fact ingestion pipeline.
         |
         |This will import a set of facts using the latest dictionary.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) }       required() text "Path to an ivory repository."
    opt[String]('t', "tmp")        action { (x, c) => c.copy(tmp = x) }        required() text "Path to store tmp data."
    opt[String]('i', "input")      action { (x, c) => c.copy(input = x) }      required() text "Path to data to import."
    opt[String]('d', "dictionary")      action { (x, c) => c.copy(dictionary = Some(x)) }      text "Name of dictionary to use."
    opt[String]('n', "namespace")  action { (x, c) => c.copy(namespace = x) }  required() text "Namespace'."
    opt[String]('z', "timezone")        action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"
    opt[Unit]('s', "singularity")     action { (_, c) => c.copy(runOnSingleMachine = true) }   text "Avoid hadoop/scoobi and import directly."

  }

  def cmd = IvoryCmd[CliArguments](parser, CliArguments("", None, "", "", "", DateTimeZone.getDefault, false), HadoopCmd { configuration => c =>
      val res = onHdfs(new Path(c.repo), c.dictionary, c.namespace, new Path(c.input), tombstone, new Path(c.tmp), c.timezone, c.runOnSingleMachine)
      res.run(configuration.modeIs(com.nicta.scoobi.core.Mode.Cluster)).map {
        case _ => s"Successfully imported '${c.input}' into '${c.repo}'"
      }
    })

  def onHdfs(repo: Path, dictionary: Option[String], namespace: String, input: Path, tombstone: List[String], tmp: Path, timezone: DateTimeZone, runOnSingleMachine: Boolean): ScoobiAction[String] =
    fatrepo.ImportWorkflow.onHdfs(repo, dictionary.map(defaultDictionaryImport(_)), importFeed(input, namespace, runOnSingleMachine), tombstone, tmp, timezone)

  def defaultDictionaryImport(dictionary: String)(repo: HdfsRepository, name: String, tombstone: List[String], tmpPath: Path): Hdfs[Unit] =
    DictionaryImporter.onHdfs(repo.root.toHdfs, repo.dictionaryByName(dictionary).toHdfs, name)

  def importFeed(input: Path, namespace: String, runOnSingleMachine: Boolean)(repo: HdfsRepository, factset: Factset, dname: String, tmpPath: Path, errorPath: Path, timezone: DateTimeZone): ScoobiAction[Unit] = for {
    dict <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dname))
    conf <- ScoobiAction.scoobiConfiguration
    _    <- if (!runOnSingleMachine)
              EavtTextImporter.onHdfs(repo, dict, factset, namespace, input, errorPath, timezone, Some(new SnappyCodec))
            else
              ScoobiAction.fromResultTIO { EavtTextImporter.onHdfsDirect(conf, repo, dict, factset, namespace, input, errorPath, timezone, identity) }
  } yield ()

}
