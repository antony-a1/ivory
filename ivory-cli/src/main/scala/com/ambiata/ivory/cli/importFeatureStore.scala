package com.ambiata.ivory.cli

import scalaz._, Scalaz._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.ingest._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.repository._

object importFeatureStore {

  lazy val configuration = new Configuration

  case class CliArguments(repo: String, name: String, path: String, tmpDirectory: String = Repository.defaultS3TmpDirectory)

  val parser = new scopt.OptionParser[CliArguments]("import-feature-store"){
    head("""
|Import a feature store into an ivory repository.
|
|This app will parse the given feature store file and if valid, import it into the given repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) } required() text
      s"Path to the repository. If the path starts with 's3://' we assume that this is a S3 repository"

    opt[String]('t', "temp-dir") action { (x, c) => c.copy(tmpDirectory = x) } optional() text
      s"Temporary directory path used to transfer data when interacting with S3. {user.home}/.s3repository by default"

    opt[String]('n', "name") action { (x, c) => c.copy(name = x) } required() text s"Name of the feature store in the repository."
    opt[String]('p', "path") action { (x, c) => c.copy(path = x) } required() text s"Hdfs path to feature store to import."
  }

  def main(args: Array[String]) {
    parser.parse(args, CliArguments("", "", "")).map { c =>
      val actions =
        if (c.repo.startsWith("s3://")) {
          val repository = Repository.fromS3(new FilePath(c.repo.replace("s3://", "")), new FilePath(c.tmpDirectory))
          FeatureStoreImporter.onS3(repository, c.name, new FilePath(c.path)).runHdfs(configuration).evalT
        }
        else
          FeatureStoreImporter.onHdfs(HdfsRepository(new Path(c.path)), c.name, new Path(c.path)).run(configuration)

      actions.run.unsafePerformIO() match {
        case Ok(v)    => println(s"Successfully imported feature store into ${c.repo} under the name ${c.name}.")
        case Error(e) => println(s"Failed to import dictionary: ${Result.asString(e)}")
      }
    }
  }
}
