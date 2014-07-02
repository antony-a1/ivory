package com.ambiata.ivory.cli

import com.ambiata.mundane.control._
import com.ambiata.ivory.storage.legacy.DictionaryTextStorage
import com.ambiata.ivory.storage.legacy.DictionaryThriftStorage.DictionaryThriftLoader
import com.ambiata.ivory.storage.repository._
import com.nicta.scoobi.Scoobi._
import scalaz._, Scalaz._, effect._

object catDictionary extends IvoryApp {

  case class CliArguments(repo: String, name: Option[String] = None, delimiter: Char = '|')

  import ScoptReaders.charRead

  val parser = new scopt.OptionParser[CliArguments]("cat-dictionary") {
    head("""
           |Print dictionary as text to standard out, delimited by '|' or explicitly set delimiter.
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")        action { (x, c) => c.copy(repo = x) }           required()             text
      s"Ivory repository to create. If the path starts with 's3://' we assume that this is a S3 repository"
    opt[String]('n', "name")        action { (x, c) => c.copy(name = Some(x)) }           optional()       text
      s"For displaying the contents of an older dictionary"
    opt[Char]('d', "delimiter")   action { (x, c) => c.copy(delimiter = x) }        optional()             text
      "Delimiter (`|` by default)"
  }

  val cmd = new IvoryCmd[CliArguments](parser, CliArguments(""), HadoopCmd { conf => {
    case CliArguments(repo, nameOpt, delim) =>
      for {
        repo <- ResultT.fromDisjunction[IO, Repository](Repository.fromUri(repo, conf).leftMap(\&/.This(_)))
        store = DictionaryThriftLoader(repo)
        dictionary <- nameOpt.cata(name => store.loadFromPath(repo.dictionaries.relativeTo(repo.root) </> name), store.load)
      } yield List(
        DictionaryTextStorage.delimitedDictionaryString(dictionary, delim)
      )
  }
  })
}
