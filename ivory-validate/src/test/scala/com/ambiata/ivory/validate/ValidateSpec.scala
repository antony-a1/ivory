package com.ambiata.ivory.validate

import org.specs2._
import org.specs2.matcher.FileMatchers
import scalaz.{DList => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.mutable._
import com.nicta.scoobi.testing.SimpleJobs
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles
import java.io.File
import java.net.URI
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.scoobi._, WireFormats._, FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import IvoryStorage._

class ValidateSpec extends HadoopSpecification with SimpleJobs with FileMatchers {
  override def isCluster = false

  "Validate feature store" >> { implicit sc: ScoobiConfiguration =>
    implicit val fs = sc.fileSystem

    val directory = path(TempFiles.createTempDir("validation").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)
    val outpath = directory + "/out"

    val dict = Dictionary("dict1", Map(FeatureId("ns1", "fid1") -> FeatureMeta(DoubleEncoding, NumericalType, "desc"),
                                      FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding, NumericalType, "desc"),
                                      FeatureId("ns2", "fid3") -> FeatureMeta(BooleanEncoding, CategoricalType, "desc")))

    dictionaryToIvory(repo, dict, dict.name).run(configuration).run.unsafePerformIO().toEither must beRight

    val facts1 = fromLazySeq(Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
                       IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 10, 1), Time(0), 10),
                       BooleanFact("eid1", FeatureId("ns2", "fid3"), Date(2012, 3, 20), Time(0), true)))
    val facts2 = fromLazySeq(Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "def")))

    persist(facts1.toIvoryFactset(repo, "factset1"), facts2.toIvoryFactset(repo, "factset2"))
    writeFactsetVersion(repo, List("factset1", "factset2")).run(sc) must beOk

    storeToIvory(repo, FeatureStore(List(FactSet("factset1", 1), FactSet("factset2", 2))), "store1").run(sc) must beOk

    Validate.validateHdfsStore(repo.root.toHdfs, "store1", "dict1", new Path(outpath), false).run(sc) must beOk

    val res = fromTextFile(outpath).run.toList
                               println(res)
    res must have size(1)
    res must contain("Not a valid double!")
    res must contain("eid1")
    res must contain("ns1")
    res must contain("fid1")
    res must contain("factset1")
  }

  "Validate fact set" >> { implicit sc: ScoobiConfiguration =>
    implicit val fs = sc.fileSystem

    val directory = path(TempFiles.createTempDir("validation").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)
    val outpath = directory + "/out"

    val dict = Dictionary("dict1", Map(FeatureId("ns1", "fid1") -> FeatureMeta(DoubleEncoding, NumericalType, "desc"),
                                      FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding, NumericalType, "desc"),
                                      FeatureId("ns2", "fid3") -> FeatureMeta(BooleanEncoding, CategoricalType, "desc")))

    dictionaryToIvory(repo, dict, dict.name).run(configuration).run.unsafePerformIO().toEither must beRight

    val facts1 = fromLazySeq(Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
                       IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 10, 1), Time(0), 10),
                       BooleanFact("eid1", FeatureId("ns2", "fid3"), Date(2012, 3, 20), Time(0), true)))

    facts1.toIvoryFactset(repo, "factset1").persist
    writeFactsetVersion(repo, List("factset1")).run(sc) must beOk

    Validate.validateHdfsFactSet(repo.root.toHdfs, "factset1", "dict1", new Path(outpath)).run(sc) must beOk

    val res = fromTextFile(outpath).run.toList
    res must have size(1)
    res must contain("Not a valid double!")
    res must contain("eid1")
    res must contain("ns1")
    res must contain("fid1")
  }
}
