package com.ambiata.ivory.ingest

import org.specs2.mutable.Specification
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._
import com.ambiata.saws.s3.S3
import com.ambiata.saws.testing.AwsMatcher._
import com.nicta.scoobi.Scoobi._
import com.amazonaws.services.s3.AmazonS3Client
import org.specs2.specification.BeforeAfterExample
import com.ambiata.ivory.alien.hdfs.HdfsS3Action
import org.apache.hadoop.conf.Configuration
import scalaz._, Scalaz._
import HdfsS3Action._
import com.ambiata.mundane.control.{ActionT, ResultT}
import scalaz.effect.IO

class FeatureStoreImporterSpec extends Specification with BeforeAfterExample {
  "A feature store can be saved on S3" >> {
    val repository = Repository.fromS3WithTemp("ambiata-dev-app", "customer" </> "ivory" </> dir, "target" </> ".s3Repository", ScoobiConfiguration())

    val actions: HdfsS3Action[Unit] =
      for {
        path <- saveFeaturestoreFile
        _    <- FeatureStoreImporter.onS3(repository, "featurestore1", path)
      } yield ()

    actions.runHdfs(new Configuration()) must beOk

    "the feature store is created on S3" ==> {
      S3.listKeys("ambiata-dev-app", s"customer/ivory/$dir/metadata/stores/featurestore1/") must beOkLike(list => list must haveSize(1))
    }
  }

  /**
   * TEST METHODS
   */
  val featurestore =
    """|customer1
       |00005
       |00004""".stripMargin

  def saveFeaturestoreFile: HdfsS3Action[FilePath] =
    HdfsS3Action.fromResultT {
      val path = new FilePath(s"target/test/$dir/featurestore1/fs1")
      Files.write(path, featurestore).map(_ => path)
    }

  def before = clean
  def after  = clean
  def clean = {
    S3.deleteAll("ambiata-dev-app", s"customer/ivory/$dir") must beOk[AmazonS3Client, Unit].or(throw new Exception("can't remove the previous repository"))
  }

  val dir = "test/"+getClass.getSimpleName

}
