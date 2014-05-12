package com.ambiata.ivory.cli

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.extract._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._

import com.nicta.scoobi.Scoobi._

import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory

import scalaz.{DList => _, _}, Scalaz._

object chord extends ScoobiApp {

  case class CliArguments(repo: String, output: String, tmp: String, errors: String, entities: String, incremental: Option[String])

  val parser = new scopt.OptionParser[CliArguments]("chord") {
    head("""
         |Extract the latest features from a given ivory repo using a list of entity id and date pairs
         |
         |The output entity ids will be of the form eid:yyyy-MM-dd
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")        action { (x, c) => c.copy(repo = x) }     required() text "Path to an ivory repository."
    opt[String]('o', "output")      action { (x, c) => c.copy(output = x) }   required() text "Path to store snapshot."
    opt[String]('t', "tmp")         action { (x, c) => c.copy(tmp = x) }      required() text "Path to store tmp data."
    opt[String]('e', "errors")      action { (x, c) => c.copy(errors = x) }   required() text "Path to store any errors."
    opt[String]('i', "incremental") action { (x, c) => c.copy(incremental = Some(x)) }   text "Path to incremental snapshot."
    opt[String]('c', "entities")    action { (x, c) => c.copy(entities = x) } required() text "Path to file containing entity/date pairs (eid|yyyy-MM-dd)."
  }

  def run {
    parser.parse(args, CliArguments("", "", "", "", "", None)).map(c => {
      val res = onHdfs(new Path(c.repo), new Path(c.output), new Path(c.tmp), new Path(c.errors), new Path(c.entities), c.incremental.map(p => new Path(p)))
      res.run(configuration).run.unsafePerformIO() match {
        case Ok(_)    => println(s"Successfully extracted chord from '${c.repo}' and stored in '${c.output}'")
        case Error(e) => println(s"Failed! - ${e}")
      }
    })
  }

  def onHdfs(repo: Path, output: Path, tmp: Path, errors: Path, entities: Path, incremental: Option[Path]): ScoobiAction[(String, String)] =
    fatrepo.ExtractChordWorkflow.onHdfs(repo, extractChord(entities, output, tmp, errors, incremental))

  def extractChord(entities: Path, outputPath: Path, tmpPath: Path, errorPath: Path, incremental: Option[Path])(repo: HdfsRepository, store: String, dictName: String): ScoobiAction[Unit] = for {
    d  <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dictName))
    _  <- Chord.onHdfs(repo.root.toHdfs, store, dictName, entities, outputPath, tmpPath, errorPath, incremental)
  } yield ()
}
