package com.ambiata.ivory.cli

import scalaz._, Scalaz._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._

import com.nicta.scoobi.Scoobi._

object createFeatureStore {

  lazy val configuration = ScoobiConfiguration()

  case class CliArguments(repo: String, name: String, sets: String, existing: Option[String], tmpDirectory: FilePath = Repository.defaultS3TmpDirectory)

  val parser = new scopt.OptionParser[CliArguments]("create-feature-store"){
    head("""
|Create a new feature store in an ivory repository.
|
|This app will create a new feature store, optionally appending an existing one to the end.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) } required() text
      s"Ivory repository to create. If the path starts with 's3://' we assume that this is a S3 repository"

    opt[String]('t', "temp-dir") action { (x, c) => c.copy(tmpDirectory = x.toFilePath) } optional() text
      s"Temporary directory path used to transfer data when interacting with S3. {user.home}/.s3repository by default"

    opt[String]('n', "name")            action { (x, c) => c.copy(name = x) } required() text s"Name of the feature store in the repository."
    opt[String]('s', "sets")            action { (x, c) => c.copy(sets = x) } required() text s"Comma separated list of fact sets to use in this feature store."
    opt[String]('e', "append-existing") action { (x, c) => c.copy(existing = Some(x)) }  text s"Name of an existing feature store to append to the end of this one."
  }

  def main(args: Array[String]) {
    parser.parse(args, CliArguments("", "", "", None)).map(c => {
      val sets = c.sets.split(",").toList

      val actions =
        if (c.repo.startsWith("s3://")) {
          val p = c.repo.replace("s3://", "").toFilePath
          val repository = Repository.fromS3WithTemp(p.rootname.path, p.fromRoot, c.tmpDirectory, configuration)
          CreateFeatureStore.onS3(repository, c.name, sets, c.existing).runHdfs(configuration).eval
        }
        else
          CreateFeatureStore.onHdfs(new Path(c.repo), c.name, sets, c.existing).run(configuration).run

        actions.unsafePerformIO() match {
        case Ok(v)    => println(s"Successfully created feature store in ${c.repo} under the name ${c.name}.")
        case Error(e) => println(s"Failed to create dictionary: ${Result.asString(e)}")
      }
    })
  }
}
