package com.ambiata.ivory.cli

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import java.util.Calendar
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.generate._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.repository._

object generateFacts extends IvoryApp {

  case class CliArguments(repo: String, flags: String, entities: Int, start: LocalDate, end: LocalDate, output: String)

  val parser = new scopt.OptionParser[CliArguments]("generate-facts"){
    head("""
|Random Fact Generator.
|
|This app generates a random fact set
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")       action { (x, c) => c.copy(repo = x) }        required() text s"Path to an ivory repository."
    opt[String]('f', "flags")      action { (x, c) => c.copy(flags = x) }       required() text s"Flags to use when generating random Facts."
    opt[Int]('n', "entities")      action { (x, c) => c.copy(entities = x) }    required() text s"Number of entities to generate random facts for."
    opt[Calendar]('s', "start")    action { (x, c) => c.copy(start = LocalDate.fromCalendarFields(x)) } required() text s"Start date."
    opt[Calendar]('e', "end")      action { (x, c) => c.copy(end = LocalDate.fromCalendarFields(x)) }   required() text s"End date."
    opt[String]('o', "output")     action { (x, c) => c.copy(output = x) }      required() text s"Hdfs path to write facts to."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", 0, LocalDate.now(), LocalDate.now(), ""), ScoobiCmd { configuration => c =>
      GenerateFacts.onHdfs(Repository.fromHdfsPath(FilePath(c.repo), configuration), c.entities, new Path(c.flags), c.start, c.end, new Path(c.output))(configuration).run(configuration).
        mapError(e => { println(s"Failed to generate dictionary: ${Result.asString(e)}"); e }).
                                                                                     map(v => List(s"Dictionary successfully written to ${c.output}."))
  })
}
