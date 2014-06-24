package com.ambiata.ivory.ingest

import org.specs2.Specification
import org.apache.hadoop.fs.Path
import com.ambiata.ivory.alien.hdfs
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.repository._
import HdfsS3Action._
import org.apache.hadoop.conf.Configuration
import com.ambiata.saws.testing.AwsMatcher._
import com.ambiata.saws.s3.S3
import com.ambiata.mundane.io._
import scalaz._, Scalaz._
import java.io.File
import org.specs2.matcher.ThrownExpectations
import com.ambiata.ivory.core._
import com.nicta.scoobi.Scoobi._

class DictionaryImporterSpec extends Specification with ThrownExpectations { def is = s2"""

 A dictionary can be imported in a ivory repository
   with a dictionary saved as a Path on Hdfs or locally and a S3 repository $e1

"""

  def e1 = {
    val repoPath = "ambiata-dev-app" </> "customer/ivory/repository1/"
    val dictionaryPath = new FilePath("target/test/dictionary.psv")
    val dictionary =
      """demo|postcode|string|categorical|Postcode|☠"""

    val onS3: HdfsS3Action[Unit] = for {
      _    <- fromHdfs(hdfs.Hdfs.writeWith(new Path(dictionaryPath.path), os => Streams.write(os, dictionary)))
      repo  = Repository.fromS3(repoPath.rootname.path, repoPath.fromRoot, ScoobiConfiguration())
      _    <- DictionaryImporter.onS3(repo, "dictionary1", dictionaryPath)
    } yield ()

    onS3.runHdfs(new Configuration) must beOk

    "the dictionary is created on S3" ==> {
      S3.listKeys("ambiata-dev-app", "customer/ivory/repository1/metadata/dictionaries/dictionary1/") must beOkLike(list => list must haveSize(1))
    }
  }
}
