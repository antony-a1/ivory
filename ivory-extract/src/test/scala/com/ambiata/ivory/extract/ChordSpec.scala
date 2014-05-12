package com.ambiata.ivory.extract

import org.specs2._
import org.specs2.matcher.{MustThrownMatchers, FileMatchers}
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
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import IvoryStorage._

class ChordSpec extends HadoopSpecification with SimpleJobs with FileMatchers with SampleFacts {
  override def isCluster = false

  "Can extract expected facts" >> { implicit sc: ScoobiConfiguration =>
    val directory = path(TempFiles.createTempDir("chord").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    val outPath = new Path(directory+"/out")
    Chord.onHdfs(repo.root.toHdfs, "store1", "dict1", new Path(directory+"/entities"), outPath, new Path(directory+"/tmp"), new Path(directory+"/err"), None).run(sc) must beOk

    valueFromSequenceFile[Fact](new Path(outPath, "thrift").toString).run.toList must containTheSameElementsAs(List(
      StringFact("eid1:2012-09-15", FeatureId("ns1", "fid1"), Date(2012, 9, 1), Time(0), "def"),
      StringFact("eid1:2012-11-01", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
      IntFact("eid2:2012-12-01", FeatureId("ns1", "fid2"), Date(2012, 11, 1), Time(0), 11)))
  }
}

trait SampleFacts extends MustThrownMatchers {
  val DICTIONARY_NAME = "dict1"

  def createEntitiesFiles(directory: String)(implicit sc: ScoobiConfiguration) = {
    implicit val fs = sc.fileSystem
    val entities = Seq("eid1|2012-09-15", "eid2|2012-12-01", "eid1|2012-11-01")

    lazy val entitiesFile = new File(directory + "/entities")
    TempFiles.writeLines(entitiesFile, entities, isRemote)
  }

  def createDictionary(repo: HdfsRepository)(implicit sc: ScoobiConfiguration) = {
    val dict = Dictionary(DICTIONARY_NAME, Map(FeatureId("ns1", "fid1") -> FeatureMeta(StringEncoding, CategoricalType, "desc"),
      FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding, NumericalType, "desc"),
      FeatureId("ns2", "fid3") -> FeatureMeta(BooleanEncoding, CategoricalType, "desc")))

    dictionaryToIvory(repo, dict, dict.name).run(sc) must beOk
    dict
  }

  def createFacts(repo: HdfsRepository)(implicit sc: ScoobiConfiguration) = {
    val facts1 =
      fromLazySeq(Seq(StringFact ("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
        StringFact ("eid1", FeatureId("ns1", "fid1"), Date(2012, 9, 1),  Time(0), "def"),
        IntFact    ("eid2", FeatureId("ns1", "fid2"), Date(2012, 10, 1), Time(0), 10),
        IntFact    ("eid2", FeatureId("ns1", "fid2"), Date(2012, 11, 1), Time(0), 11),
        BooleanFact("eid3", FeatureId("ns2", "fid3"), Date(2012, 3, 20), Time(0), true)))

    val facts2 =
      fromLazySeq(Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 9, 1), Time(0), "ghi")))

    persist(facts1.toIvoryFactset(repo, Factset("factset1")), facts2.toIvoryFactset(repo, Factset("factset2")))
    writeFactsetVersion(repo, List(Factset("factset1"), Factset("factset2"))).run(sc) must beOk

    storeToIvory(repo, FeatureStore(List(PrioritizedFactset(Factset("factset1"), 1), PrioritizedFactset(Factset("factset2"), 2))), "store1").run(sc) must beOk

  }

  def createAll(dirName: String)(implicit sc: ScoobiConfiguration) = {
    val directory = path(TempFiles.createTempDir(dirName).getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)
    directory
  }
}
