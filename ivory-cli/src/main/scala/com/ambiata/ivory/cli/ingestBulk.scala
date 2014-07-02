package com.ambiata.ivory.cli

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.ingest.{DictionaryImporter, EavtTextImporter}
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._

import org.apache.hadoop.io.compress._
import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory
import org.joda.time.DateTimeZone

import scalaz.{DList => _, _}, Scalaz._

object ingestBulk extends IvoryApp {

  case class CliArguments(repo: String, dictionary: Option[String], input: String, tmp: String, timezone: DateTimeZone, optimal: Long, codec: Option[CompressionCodec])

  val parser = new scopt.OptionParser[CliArguments]("ingest-bulk") {
    head("""
         |Fact ingestion pipeline.
         |
         |This will import a set of facts using the latest dictionary.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[Unit]('n', "no-compression")         action { (_, c) => c.copy(codec = None) }    text "Don't use compression."

    opt[String]('r', "repo")                 action { (x, c) => c.copy(repo = x) }       required() text "Path to an ivory repository."
    opt[String]('t', "tmp")                  action { (x, c) => c.copy(tmp = x) }        required() text "Path to store tmp data."
    opt[String]('i', "input")                action { (x, c) => c.copy(input = x) }      required() text "Path to data to import."
    opt[Long]('o', "optimal-input-chunk")    action { (x, c) => c.copy(optimal = x) }      text "Optimal size (in bytes) of input chunk.."
    opt[String]('d', "dictionary")           action { (x, c) => c.copy(dictionary = Some(x)) }      text "Name of dictionary to use."
    opt[String]('z', "timezone")             action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"

  }

  type Namespace = String
  type Parts = String

  def cmd = IvoryCmd[CliArguments](parser,
      CliArguments("", None, "", "", DateTimeZone.getDefault, 1024 * 1024 * 256 /* 256MB */, Some(new SnappyCodec)),
      ScoobiCmd(configuration => c => {
      val res = onHdfs(new Path(c.repo), c.dictionary, new Path(c.input), new Path(c.tmp), c.timezone, c.optimal, c.codec)
      res.run(configuration).map {
        case f => List(s"Successfully imported '${c.input}' as ${f} into '${c.repo}'")
      }
    }))

  def onHdfs(repo: Path, dictionary: Option[String], input: Path, tmp: Path, timezone: DateTimeZone, optimal: Long, codec: Option[CompressionCodec]): ScoobiAction[Factset] =
    fatrepo.ImportWorkflow.onHdfs(repo, dictionary.map(defaultDictionaryImport(_)), importFeed(input, optimal, codec), tmp, timezone)

  def defaultDictionaryImport(dictionary: String)(repo: HdfsRepository, name: String, tmpPath: Path): Hdfs[Unit] =
    DictionaryImporter.onHdfs(repo.root.toHdfs, repo.dictionaryByName(dictionary).toHdfs, name)

  def importFeed(input: Path, optimal: Long, codec: Option[CompressionCodec])(repo: HdfsRepository, factset: Factset, dname: String, tmpPath: Path, errorPath: Path, timezone: DateTimeZone): ScoobiAction[Unit] = for {
    dict <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dname))
    list <- listing(input)
    conf <- ScoobiAction.scoobiConfiguration
    _    <- EavtTextImporter.onHdfsBulk(repo, dict, factset, list.map(_._1), input, errorPath, timezone, list, optimal, codec)
  } yield ()

  def listing(in: Path): ScoobiAction[List[(Namespace, Long)]] = ScoobiAction.fromHdfs(for {
    namespaces <- Hdfs.globPaths(in).map(_.map(_.getName))
    parts      <- namespaces.traverse(namespace => for {
      all <- Hdfs.globPathsRecursively(new Path(in, namespace))
      sizes <- all.traverse(Hdfs.size)
    } yield (namespace -> sizes.sum))
  } yield parts)
}
