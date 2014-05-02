package com.ambiata.ivory.cli

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.joda.time.DateTimeZone
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.core._
import com.ambiata.mundane.io.FilePath
import scala.Some
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.ingest.EavtTextImporter
import com.ambiata.mundane.control.{Error, Ok}
import scalaz._, Scalaz._
import ScoobiS3EMRAction._

object importFacts extends ScoobiApp {

  case class CliArguments(repositoryPath: String = "",
                          dictionary: String = "",
                          factset: String    = "",
                          namespace: String  = "",
                          input: String      = "",
                          errors: Option[Path] = None,
                          timezone: DateTimeZone = DateTimeZone.getDefault,
                          tmpDirectory: String = Repository.defaultS3TmpDirectory)

  val parser = new scopt.OptionParser[CliArguments]("import-facts"){
    head("""
           |Facts Importer.
           |
           |Expected format: <entity id>|<feature id>|<value>|<yyyy-MM-dd HH:mm:ss>
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repository").action { (x, c) => c.copy(repositoryPath = x) }.required.
      text (s"Ivory repository to import features into. If the path starts with 's3://' we assume that this is a S3 repository")

    opt[String]('t', "temp-dir") action { (x, c) => c.copy(tmpDirectory = x) } optional() text
      s"Temporary directory path used to transfer data when interacting with S3. {user.home}/.s3repository by default"

    opt[String]('d', "dictionary")      action { (x, c) => c.copy(dictionary = x) }      required() text s"Dictionary name, used to get encoding of fact."
    opt[String]('f', "factset")         action { (x, c) => c.copy(factset = x) }   required() text s"Fact set name to import the feature into."
    opt[String]('n', "namespace")       action { (x, c) => c.copy(namespace = x) } required() text s"Namespace to import features into."
    opt[String]('i', "input")           action { (x, c) => c.copy(input = x)   }   required() text s"path to read EAVT text files from."
    opt[String]('e', "errors")          action { (x, c) => c.copy(errors = Some(new Path(x)))  }  optional() text s"optional path to persist errors in (not loaded to s3)"
    opt[String]('z', "timezone")        action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"
  }

  def run() {
    parser.parse(args, CliArguments()).map { c =>
      val actions: ScoobiS3EMRAction[Unit] = if (c.repositoryPath.startsWith("s3://")) {
        // import to S3
        val repository = Repository.fromS3(new FilePath(c.repositoryPath.replace("s3://", "")), new FilePath(c.tmpDirectory))
        for {
          dictionary <- ScoobiS3EMRAction.fromHdfsS3(DictionariesS3Loader(repository).load(c.dictionary))
          _          <- EavtTextImporter.onS3(repository, dictionary, c.factset, c.namespace, new FilePath(c.input), c.timezone, Some(new SnappyCodec))
        } yield ()
      } else {
        // import to Hdfs only
        val repository = HdfsRepository(new Path(c.repositoryPath))
        for {
          dictionary <- ScoobiS3EMRAction.fromHdfs(InternalDictionaryLoader(repository, c.dictionary).load)
          _          <- ScoobiS3EMRAction.fromScoobiAction(
            EavtTextImporter.onHdfs(repository, dictionary, c.factset, c.namespace,
              new Path(c.input), c.errors.getOrElse(new Path("errors")), c.timezone, Some(new SnappyCodec)))
        } yield ()
      }

      actions.runScoobiAws(configuration).unsafePerformIO match {
        case Ok(_)    => println(s"successfully imported into ${c.repositoryPath}")
        case Error(e) => sys.error(s"failed! $e")
      }
    }
  }
}
