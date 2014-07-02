package com.ambiata.ivory.cli

import scalaz._, Scalaz._, effect._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.ingest._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.repository._

import com.nicta.scoobi.Scoobi._

object importDictionary extends IvoryApp {

  case class CliArguments(repo: String, path: String, update: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("import-dictionary"){
    head("""
|Import dictionary into ivory.
|
|This app will parse the given dictionary and if valid, import it into the given repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) } required() text
      s"Path to the repository. If the path starts with 's3://' we assume that this is a S3 repository"

    opt[String]('p', "path") action { (x, c) => c.copy(path = x) } required() text s"Hdfs path to either a single dictionary file or directory of files to import."
    opt[String]('u', "update") action { (x, c) => c.copy(update = true) } optional() text s"Update the existing dictionary with extra values."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", update = false), HadoopCmd { configuration => c =>
      for {
        repository <- ResultT.fromDisjunction[IO, Repository](Repository.fromUri(c.repo, configuration).leftMap(\&/.This(_)))
        newPath <- DictionaryImporter.fromPath(repository, FilePath(c.path),
          if (c.update) DictionaryImporter.Update else DictionaryImporter.Override
        )
      } yield {
        List(s"Successfully imported dictionary ${c.path} into ${c.repo} under $newPath.")
      }
  })
}
