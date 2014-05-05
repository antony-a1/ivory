package com.ambiata.ivory.ingest

import org.specs2._
import org.specs2.matcher.FileMatchers
import scalaz.{DList => _, _}, Scalaz._, \&/._
import org.joda.time.DateTimeZone
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.mutable._
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.{TempFiles, SimpleJobs}
import java.io.File
import java.net.URI
import org.apache.hadoop.fs.Path

import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.testing.ResultTIOMatcher._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.mundane.io._
import org.specs2.specification.{Fixture, FixtureExample}
import org.specs2.execute.{Result, AsResult}

class EavtTextImporterSpec extends HadoopSpecification with SimpleJobs with FileMatchers {
  override def isCluster = false

  "Scoobi job runs and creates expected data" >> { setup: Setup =>
    import setup._

    saveInputFile

    // create a repository
    val directory = path(TempFiles.createTempDir("eavtimporter").getPath)
    val input = directory + "/input"
    val repository = Repository.fromHdfsPath(directory </> "repo", ScoobiRun(sc))

    val errors = new Path(directory, "errors")
    // run the scoobi job to import facts on Hdfs
    EavtTextImporter.onHdfs(repository, dictionary, "factset1", "ns1", new Path(input), errors, DateTimeZone.getDefault, identity).run(sc) must beOk

    val expected = List(
      StringFact("pid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1),  Time(10), "v1"),
      IntFact(   "pid1", FeatureId("ns1", "fid2"), Date(2012, 10, 15), Time(20), 2),
      DoubleFact("pid1", FeatureId("ns1", "fid3"), Date(2012, 3, 20),  Time(30), 3.0))


    factsFromIvoryFactset(repository, "factset1").map(_.run.collect { case \/-(r) => r}).run(sc) must beOkLike(_ must containTheSameElementsAs(expected))
  }

  "When there are errors, they must be saved as a Thrift record containing the full record + the error message" >> { setup: Setup =>
    import setup._
    // save an input file containing errors
    saveInputFileWithErrors
    val errors = new Path(directory, "errors")

    // run the scoobi job to import facts on Hdfs
    EavtTextImporter.onHdfs(repository, dictionary, "factset1", "ns1", new Path(input), errors, DateTimeZone.getDefault, identity).run(sc) must beOk
    valueFromSequenceFile[ParseError](errors.toString).run must not(beEmpty)
  }

  implicit def setup: Fixture[Setup] = new Fixture[Setup] {
    def apply[R : AsResult](f: Setup => R): Result = fixtureContext { sc1: ScoobiConfiguration =>
      f(new Setup { implicit lazy val sc = sc1 })
    }
  }
}

trait Setup {
  implicit def sc: ScoobiConfiguration
  implicit lazy val fs = sc.fileSystem

  val directory = path(TempFiles.createTempDir("eavtimporter").getPath)
  val input = directory + "/input"
  val repository = Repository.fromHdfsPath(directory </> "repo", ScoobiRun(sc))

  val dictionary =
    Dictionary("dict",
      Map(FeatureId("ns1", "fid1") -> FeatureMeta(StringEncoding, CategoricalType, "abc"),
          FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding,    NumericalType, "def"),
          FeatureId("ns1", "fid3") -> FeatureMeta(DoubleEncoding, NumericalType, "ghi")))

  def saveInputFile = {
    val raw = List("pid1|fid1|v1|2012-10-01 00:00:10",
      "pid1|fid2|2|2012-10-15 00:00:20",
      "pid1|fid3|3.0|2012-03-20 00:00:30")

    TempFiles.writeLines(new File(input), raw, isRemote)
  }

  def saveInputFileWithErrors = {
    val raw = List("pid1|fid1|v1|2012-10-01 00:00:10",
                   "pid1|fid2|x|2012-10-15 00:00:20",
                   "pid1|fid3|3.0|2012-03-20 00:00:30")

    TempFiles.writeLines(new File(input), raw, isRemote)
  }

}
