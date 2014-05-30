package com.ambiata.ivory
package ingest

import org.specs2.Specification
import org.apache.hadoop.fs.Path
import com.ambiata.ivory.alien.hdfs
import com.ambiata.ivory.alien.hdfs._
import HdfsS3Action._
import org.apache.hadoop.conf.Configuration
import com.ambiata.saws.testing.AwsMatcher._
import com.ambiata.saws.s3.S3
import com.ambiata.mundane.io._
import scalaz.{DList => _,_}, Scalaz._
import java.io.File
import org.specs2.matcher.{FileMatchers, ThrownExpectations}
import com.ambiata.ivory.core._
import com.nicta.scoobi.testing.{SimpleJobs, HadoopSpecification, TempFiles}
import com.nicta.scoobi._
import testing.TestFiles._
import Scoobi._
import com.nicta.scoobi.core.ScoobiConfiguration
import com.ambiata.ivory.scoobi.ScoobiS3EMRAction
import ScoobiS3EMRAction._
import org.specs2.specification.{BeforeAfterExample, BeforeExample}
import com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io.FilePath
import com.ambiata.ivory.core.Fact
import org.joda.time.DateTimeZone

class FactsetS3ImporterSpec extends HadoopSpecification with ThrownExpectations with FileMatchers with BeforeAfterExample { def is = s2"""

 A facts set can be imported in an ivory S3 repository
   with a fact saved as a local path and a S3 repository / dictionary $e1

"""

  def e1 = { implicit sc: SC =>
    val repository = Repository.fromS3WithTemp("ambiata-dev-app", "customer" </> "ivory" </> dir, "target" </> ".s3Repository", ScoobiConfiguration())

    val actions: ScoobiS3EMRAction[Unit] =
    for {
      dictionary <- createDictionary(repository)
      path       <- saveFactsetFile
      _          <- EavtTextImporter.onS3(repository, dictionary, Factset("factset1"), "customer", new FilePath(path.toString), DateTimeZone.getDefault, None)
    } yield ()

    actions.runScoobi(sc) must beOk

    "the factset is created on S3" ==> {
      S3.listKeys("ambiata-dev-app", s"customer/ivory/$dir/factsets/factset1/customer/") must beOkLike(list => list must haveSize(1))
    }
  }

  def createDictionary(repository: S3Repository) = {
    val dictionaryPath = new FilePath(s"target/$dir/dictionary.psv")
    val dictionary = """customer|has.email|boolean|categorical|true if the customer has an email address|☠"""

    for {
      _          <- ScoobiS3EMRAction.fromHdfs(hdfs.Hdfs.writeWith(new Path(dictionaryPath.path), os => Streams.write(os, dictionary)))
      dictionary <- ScoobiS3EMRAction.fromHdfsS3(DictionaryImporter.onS3(repository, "dictionary1", dictionaryPath))
    } yield dictionary
  }

  val expected = List(
    BooleanFact("6207777", FeatureId("customer", "has.email"), Date(1979, 10, 17), Time(10), true),
    BooleanFact("3916666", FeatureId("customer", "has.email"), Date(1979, 10, 17), Time(20), false))

  val emails =
    """|6207777|has.email|true|1979-10-17 00:00:10
       |3916666|has.email|false|1979-10-17 00:00:20""".stripMargin

  def saveFactsetFile(implicit sc: SC) = ScoobiS3EMRAction.safe {
    val directory = path(TempFiles.createTempDir(dir).getPath)
    val input = directory + "/input"

    TempFiles.writeLines(new File(input), emails.split("\n"), isRemote)(sc.fileSystem)
    new Path(input)
  }

  def before = clean
  def after  = clean
  def clean = {
    S3.deleteAll("ambiata-dev-app", s"customer/ivory/$dir") must beOk[AmazonS3Client, Unit].or(throw new Exception("can't remove the previous repository"))
  }

  override def isCluster = false

  type SC = ScoobiConfiguration

  val dir = "test-"+getClass.getSimpleName
}
