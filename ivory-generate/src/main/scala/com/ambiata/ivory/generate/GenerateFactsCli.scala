package com.ambiata.ivory.generate

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import java.util.Calendar
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._

object GenerateFactsCli extends ScoobiApp {

  case class CliArguments(dictionary: String, flags: String, entities: Int, start: LocalDate, end: LocalDate, output: String)

  val parser = new scopt.OptionParser[CliArguments]("GenerateDictionaryCli"){
    head("""
|Random Fact Generator.
|
|This app generates a random fact set
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('d', "dictionary") action { (x, c) => c.copy(dictionary = x) }  required() text s"Dictionary to generate facts from."
    opt[String]('f', "flags")      action { (x, c) => c.copy(flags = x) }       required() text s"Flags to use when generating random Facts."
    opt[Int]('n', "entities")      action { (x, c) => c.copy(entities = x) }    required() text s"Number of entities to generate random facts for."
    opt[Calendar]('s', "start")    action { (x, c) => c.copy(start = LocalDate.fromCalendarFields(x)) } required() text s"Start date."
    opt[Calendar]('e', "end")      action { (x, c) => c.copy(end = LocalDate.fromCalendarFields(x)) }   required() text s"End date."
    opt[String]('o', "output")     action { (x, c) => c.copy(output = x) }      required() text s"Hdfs path to write facts to."
  }

  def run() {
    parser.parse(args, CliArguments("", "", 0, LocalDate.now(), LocalDate.now(), "")).map(c =>
      GenerateFacts.onHdfs(c.entities, new Path(c.dictionary), new Path(c.flags), c.start, c.end, new Path(c.output))(configuration).run(configuration).run.unsafePerformIO() match {
        case Ok(v)    => println(s"Dictionary successfully written to ${c.output}.")
        case Error(e) => println(s"Failed to generate dictionary: ${Result.asString(e)}")
      }
    )
  }
}
