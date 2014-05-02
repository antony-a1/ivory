package com.ambiata.ivory.generate

import scalaz._, Scalaz._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.alien.hdfs._

object GenerateDictionaryCli {

  lazy val configuration = new Configuration

  case class CliArguments(namespaces: Int, features: Int, output: String)

  val parser = new scopt.OptionParser[CliArguments]("GenerateDictionaryCli"){
    head("""
|Random Dictionary Generator.
|
|This app generates a random dictionary and feature flag files used to generate random features
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[Int]('n', "namespaces") action { (x, c) => c.copy(namespaces = x) } required() text s"Number of namespaces to generate."
    opt[Int]('f', "features")   action { (x, c) => c.copy(features = x) }   required() text s"Number of features to generate."
    opt[String]('o', "output")  action { (x, c) => c.copy(output = x) }     required() text s"Hdfs path to write dictionary to."
  }

  def main(args: Array[String]) {
    parser.parse(args, CliArguments(0, 0, "")).map(c =>
      generate(c.namespaces, c.features, new Path(c.output, "dictionary"), new Path(c.output, "flags")).run(configuration).run.unsafePerformIO() match {
        case Ok(v)    => println(s"Dictionary successfully written to ${c.output}.")
        case Error(e) => println(s"Failed to generate dictionary: ${Result.asString(e)}")
      }
    )
  }

  def generate(namespaces: Int, features: Int, dictPath: Path, flagsPath: Path): Hdfs[Unit] = for {
    _    <- GenerateDictionary.onHdfs(namespaces, features, dictPath)
    dict <- DictionaryTextStorage.dictionaryFromHdfs(dictPath).mapErrorString(e => s"Generated dictionary could not be read! $e")
    _    <- GenerateFeatureFlags.onHdfs(dict, flagsPath)
  } yield ()
}
