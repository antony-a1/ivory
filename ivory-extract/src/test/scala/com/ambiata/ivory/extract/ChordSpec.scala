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
import com.ambiata.ivory.alien.hdfs._
import IvoryStorage._

class ChordSpec extends Specification with FileMatchers with SampleFacts { def is = s2"""

ChordSpec
-----------

  Can extract expected facts  $e1

"""
  def e1 = {
    implicit val sc: ScoobiConfiguration = ScoobiConfiguration()
    val directory = path(TempFiles.createTempDir("chord").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    Hdfs.mkdir(repo.snapshots.toHdfs).run(sc) must beOk

    val outPath = new Path(directory+"/out")
    Chord.onHdfs(repo.root.toHdfs, new Path(directory+"/entities"), outPath, new Path(directory+"/tmp"), true, None).run(sc) must beOk

    valueFromSequenceFile[Fact](outPath.toString).run.toList must containTheSameElementsAs(List(
      StringFact("eid1:2012-09-15", FeatureId("ns1", "fid1"), Date(2012, 9, 1), Time(0), "def"),
      StringFact("eid1:2012-11-01", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
      IntFact("eid2:2012-12-01", FeatureId("ns1", "fid2"), Date(2012, 11, 1), Time(0), 11)))
  }
}

trait SampleFacts extends MustThrownMatchers {

  def createEntitiesFiles(directory: String)(implicit sc: ScoobiConfiguration) = {
    implicit val fs = sc.fileSystem
    val entities = Seq("eid1|2012-09-15", "eid2|2012-12-01", "eid1|2012-11-01")

    lazy val entitiesFile = new File(directory + "/entities")
    TempFiles.writeLines(entitiesFile, entities, isRemote)
  }

  def createDictionary(repo: HdfsRepository) = {
    val dict = Dictionary(Map(FeatureId("ns1", "fid1") -> FeatureMeta(StringEncoding, CategoricalType, "desc"),
      FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding, NumericalType, "desc"),
      FeatureId("ns2", "fid3") -> FeatureMeta(BooleanEncoding, CategoricalType, "desc")))

    dictionaryToIvory(repo, dict) must beOk
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

    persist(facts1.toIvoryFactset(repo, Factset("factset1"), None), facts2.toIvoryFactset(repo, Factset("factset2"), None))
    writeFactsetVersion(repo, List(Factset("factset1"), Factset("factset2"))).run(sc) must beOk

    storeToIvory(repo, FeatureStore(List(PrioritizedFactset(Factset("factset1"), Priority(1)), PrioritizedFactset(Factset("factset2"), Priority(2)))), "store1").run(sc) must beOk

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
