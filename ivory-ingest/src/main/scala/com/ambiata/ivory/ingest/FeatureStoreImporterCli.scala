package com.ambiata.ivory.ingest

import scalaz._, Scalaz._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._

object FeatureStoreImporterCli {

  lazy val configuration = new Configuration

  case class CliArguments(repositoryPath: String = "", storeName: String = "", storePath: String = "")

  val parser = new scopt.OptionParser[CliArguments]("FeatureStoreImporterCli"){
    head("""
|Import a feature store into an ivory repository.
|
|This app will parse the given feature store file and if valid, import it into the given repository.
|""".stripMargin)

    help("help") text "shows this usage text"

    opt[String]('r', "repository").action { (x, c) => c.copy(repositoryPath = x) }.required.
      text (s"Ivory repository to import the feature store into. If the path starts with 's3://' we assume that this is a S3 repository")
    opt[String]('n', "name") action { (x, c) => c.copy(storeName = x) } required() text s"Name of the feature store in the repository."
    opt[String]('p', "path") action { (x, c) => c.copy(storePath = x) } required() text s"Hdfs path to feature store to import."
  }

  def main(args: Array[String]) {
    parser.parse(args, CliArguments()).map { c => 
      val action = 
        if (c.repositoryPath.startsWith("s3://"))
          FeatureStoreImporter.onS3(Repository.fromS3(new FilePath(c.repositoryPath)), c.storeName, new FilePath(c.storePath.replace("s3://", "")))
        else
          HdfsS3Action.fromHdfs(FeatureStoreImporter.onHdfs(Repository.fromHdfsPath(new Path(c.repositoryPath)), c.storeName, new Path(c.storePath)))
      
      action.runHdfsAws(configuration).unsafePerformIO() match {
        case Ok(v)    => println(s"Successfully imported feature store into ${c.repositoryPath} under the name ${c.storeName}.")
        case Error(e) => println(s"Failed to import dictionary: ${Result.asString(e)}")
      }
    }
  }
}
