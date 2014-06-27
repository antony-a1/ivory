package com.ambiata.ivory.cli

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.extract._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.apache.commons.logging.LogFactory

import scalaz.{DList => _, _}, Scalaz._

object chord extends IvoryApp {

  case class CliArguments(repo: String, output: String, tmp: String, entities: String, takeSnapshot: Boolean, pivot: Boolean, delim: Char, tombstone: String)

  implicit val charRead: scopt.Read[Char] =
    scopt.Read.reads(str => {
      val chars = str.toCharArray
      chars.length match {
        case 0 => throw new IllegalArgumentException(s"'${str}' can not be empty!")
        case 1 => chars(0)
        case l => throw new IllegalArgumentException(s"'${str}' is not a char!")
      }
    })

  val parser = new scopt.OptionParser[CliArguments]("extract-chord") {
    head("""
         |Extract the latest features from a given ivory repo using a list of entity id and date pairs
         |
         |The output entity ids will be of the form eid:yyyy-MM-dd
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")     action { (x, c) => c.copy(repo = x) }      required() text "Path to an ivory repository."
    opt[String]('o', "output")   action { (x, c) => c.copy(output = x) }    required() text "Path to store snapshot."
    opt[String]('t', "tmp")      action { (x, c) => c.copy(tmp = x) }       required() text "Path to store tmp data."
    opt[String]('c', "entities") action { (x, c) => c.copy(entities = x) }  required() text "Path to file containing entity/date pairs (eid|yyyy-MM-dd)."
    opt[Unit]("no-snapshot")     action { (x, c) => c.copy(takeSnapshot = false) }     text "Do not take a new snapshot, just any existing."
    opt[Unit]("pivot")           action { (x, c) => c.copy(pivot = true) }             text "Pivot the output data."
    opt[Char]("delim")           action { (x, c) => c.copy(delim = x) }                text "Delimiter for pivot file, default '|'."
    opt[String]("tombstone")     action { (x, c) => c.copy(tombstone = x) }            text "Tombstone for pivot file, default 'NA'."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", "", "", true, false, '|', "NA"), ScoobiCmd { configuration => c =>
      val res = onHdfs(new Path(c.repo), new Path(c.output), new Path(c.tmp), new Path(c.entities), c.takeSnapshot, c.pivot, c.delim, c.tombstone)
      res.run(configuration).map {
        case _ => List(s"Successfully extracted chord from '${c.repo}' and stored in '${c.output}'")
      }
    })

  def onHdfs(repoPath: Path, outputPath: Path, tmpPath: Path, entitiesPath: Path, takeSnapshot: Boolean, pivot: Boolean, delim: Char, tombstone: String): ScoobiAction[Unit] = for {
    tout <- ScoobiAction.value(new Path(outputPath, "thrift"))
    dout <- ScoobiAction.value(new Path(outputPath, "dense"))
    _    <- Chord.onHdfs(repoPath, entitiesPath, tout, new Path(tmpPath, "chord"), takeSnapshot, Some(new SnappyCodec))
    _    <- if(pivot) {
              println("Pivoting extracted chord in '${tout}' to '${dout}'")
              Pivot.onHdfs(tout, dout, new Path(tout, ".dictionary"), delim, tombstone)
            } else ScoobiAction.ok(())
  } yield ()
}
