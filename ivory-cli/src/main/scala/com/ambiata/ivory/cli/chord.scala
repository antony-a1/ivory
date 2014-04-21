package com.ambiata.ivory.cli

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.chord._
import com.ambiata.ivory.repository._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage._

import com.nicta.scoobi.Scoobi._

import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory

import scalaz.{DList => _, _}, Scalaz._

object chord extends ScoobiApp {

  case class CliArguments(repo: String, output: String, errors: String, entities: String, storer: ChordStorer)

  implicit val chordStorerRead: scopt.Read[ChordStorer] =
  scopt.Read.reads(str => str match {
    case "eavttext"     => EavtTextChordStorer
    case "denserowtext" => DenseRowTextChordStorer
    case s              => throw new IllegalArgumentException(s"Storer '${s}' not found!")
  })

  val parser = new scopt.OptionParser[CliArguments]("chord") {
    head("""
         |Extract the latest features from a given ivory repo using a list of entitiy id and date pairs
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")        action { (x, c) => c.copy(repo = x) }     required() text "Path to an ivory repository."
    opt[String]('o', "output")      action { (x, c) => c.copy(output = x) }   required() text "Path to store snapshot."
    opt[String]('e', "errors")      action { (x, c) => c.copy(errors = x) }   required() text "Path to store any errors."
    opt[String]('c', "entities")    action { (x, c) => c.copy(entities = x) } required() text "Path to file containing entity/date pairs (eid|yyyy-MM-dd)."
    opt[ChordStorer]('s', "storer") action { (x, c) => c.copy(storer = x) }              text "Name of storer to use 'eavttext', or 'denserowtext'"
  }

  def run {
    parser.parse(args, CliArguments("", "", "", "", EavtTextChordStorer)).map(c => {
      val res = onHdfs(new Path(c.repo), new Path(c.output), new Path(c.errors), new Path(c.entities), c.storer)
      res.run(configuration).run.unsafePerformIO() match {
        case Ok(_)    => println(s"Successfully extracted chord from '${c.repo}' and stored in '${c.output}'")
        case Error(e) => println(s"Failed! - ${e}")
      }
    })
  }

  def onHdfs(repo: Path, output: Path, errors: Path, entities: Path, storer: ChordStorer): ScoobiAction[(String, String)] =
    fatrepo.ExtractChordWorkflow.onHdfs(repo, extractChord(entities, output, errors, storer))

  def extractChord(entities: Path, outputPath: Path, errorPath: Path, storer: ChordStorer)(repo: HdfsRepository, store: String, dictName: String): ScoobiAction[Unit] = for {
    d  <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dictName))
    s   = storer match {
      case DenseRowTextChordStorer => DenseRowTextStorage.DenseRowTextStorer(outputPath.toString, d)
      case EavtTextChordStorer     => EavtTextStorage.EavtTextStorerV1(outputPath.toString)
    }
    _  <- Chord.onHdfs(repo.path, store, dictName, entities, outputPath, errorPath, s)
  } yield ()
}

sealed trait ChordStorer
case object DenseRowTextChordStorer extends ChordStorer
case object EavtTextChordStorer extends ChordStorer
