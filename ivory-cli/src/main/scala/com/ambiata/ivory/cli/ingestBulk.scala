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

object ingestBulk extends ScoobiApp {

  val tombstone = List("☠")

  case class CliArguments(repo: String, dictionary: Option[String], input: String, tmp: String, timezone: DateTimeZone, codec: Option[CompressionCodec])

  val parser = new scopt.OptionParser[CliArguments]("ingest") {
    head("""
         |Fact ingestion pipeline.
         |
         |This will import a set of facts using the latest dictionary.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('n', "no-compression") action { (x, c) => c.copy(codec = None) }       required() text "Don't use compression."
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) }       required() text "Path to an ivory repository."
    opt[String]('t', "tmp")        action { (x, c) => c.copy(tmp = x) }        required() text "Path to store tmp data."
    opt[String]('i', "input")      action { (x, c) => c.copy(input = x) }      required() text "Path to data to import."
    opt[String]('d', "dictionary")      action { (x, c) => c.copy(dictionary = Some(x)) }      text "Name of dictionary to use."
    opt[String]('z', "timezone")        action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"

  }

  type Namespace = String
  type Parts = String

  def run {
    parser.parse(args, CliArguments("", None, "", "", DateTimeZone.getDefault, Some(new SnappyCodec))).map(c => {
      val res = onHdfs(new Path(c.repo), c.dictionary, new Path(c.input), tombstone, new Path(c.tmp), c.timezone, c.codec)
      res.run(configuration.modeIs(com.nicta.scoobi.core.Mode.Cluster) <| { c =>
        // MR1
        c.set("mapred.compress.map.output", "true")
        c.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec")

        // YARN
        c.set("mapreduce.map.output.compress", "true")
        c.set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")
      }) .run.unsafePerformIO() match {
        case Ok(_)    => println(s"Successfully imported '${c.input}' into '${c.repo}'")
        case Error(e) => println(s"Failed! - ${Result.asString(e)}")
      }
    })
  }

  def onHdfs(repo: Path, dictionary: Option[String], input: Path, tombstone: List[String], tmp: Path, timezone: DateTimeZone, codec: Option[CompressionCodec]): ScoobiAction[String] =
    fatrepo.ImportWorkflow.onHdfs(repo, dictionary.map(defaultDictionaryImport(_)), importFeed(input, codec), tombstone, tmp, timezone)

  def defaultDictionaryImport(dictionary: String)(repo: HdfsRepository, name: String, tombstone: List[String], tmpPath: Path): Hdfs[Unit] =
    DictionaryImporter.onHdfs(repo.root.toHdfs, repo.dictionaryByName(dictionary).toHdfs, name)

  def importFeed(input: Path, codec: Option[CompressionCodec])(repo: HdfsRepository, factset: Factset, dname: String, tmpPath: Path, errorPath: Path, timezone: DateTimeZone): ScoobiAction[Unit] = for {
    dict <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dname))
    list <- listing(input)
    _    = list.foreach({ case (n, d) => { println(s"namespace[$n] --"); d.foreach(x => println(s"  $x")) }})
    _    = partitions(list).foreach({ case (n, d) => println(s"part $n / $d") })
    conf <- ScoobiAction.scoobiConfiguration
    _    <- EavtTextImporter.onHdfsBulk(repo, dict, factset, list.map(_._1), input, errorPath, timezone, partitions(list), codec)
  } yield ()

  def partitions(ns: List[(Namespace, List[Parts])]): Map[String, Int] =
    ns.flatMap({ case (n, ps) => ps.map(p => s"${n}/${p.replace("-", "/")}") }).zipWithIndex.toMap

  def listing(in: Path): ScoobiAction[List[(Namespace, List[Parts])]] = ScoobiAction.fromHdfs(for {
    namespaces <- Hdfs.globPaths(in).map(_.map(_.getName))
    parts      <- namespaces.traverse(namespace => Hdfs.globPaths(new Path(in, namespace)).map(paths => (namespace -> paths.map(_.getName))))
  } yield parts)

}
