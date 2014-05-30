package com.ambiata.ivory.validate

import org.specs2._
import org.specs2.matcher.FileMatchers
import scalaz.{DList => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import com.nicta.scoobi.testing.mutable._
import com.nicta.scoobi.testing.SimpleJobs
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.alien.hdfs._
import WireFormats._
import FactFormats._

class FactDiffSpec extends HadoopSpecification with SimpleJobs with FileMatchers {
  override def isCluster = false

  "FactDiff finds difference with all facts" >> { implicit sc: ScoobiConfiguration =>
    val facts1 = fromLazySeq(
                   Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
                       IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 10, 1), Time(0), 10),
                       BooleanFact("eid1", FeatureId("ns2", "fid3"), Date(2012, 3, 20), Time(0), true)))
    val facts2 = fromLazySeq(
                   Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abcd"),
                       IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 10, 1), Time(0), 101),
                       BooleanFact("eid1", FeatureId("ns2", "fid3"), Date(2012, 3, 20), Time(0), false)))

    val directory: String = path(TempFiles.createTempDir("factdiff").getPath).pp
    val input1 = directory + "/1"
    val input2 = directory + "/2"
    val output = directory + "/out"
    val errors = directory + "/errors"

    persist(PartitionFactThriftStorageV1.PartitionedFactThriftStorer(input1, None).storeScoobi(facts1),
            PartitionFactThriftStorageV1.PartitionedFactThriftStorer(input2, None).storeScoobi(facts2))

    FactDiff.scoobiJob(input1, input2, output, errors).run(sc) must beOk

    val out = fromTextFile(output).run.toList
    out must have size(6)

    Hdfs.readWith(new Path(errors), is => Streams.read(is)).run(sc) must beOkValue("")
  }

  "FactDiff finds no difference" >> { implicit sc: ScoobiConfiguration =>
    val facts1 = fromLazySeq(
                   Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
                       IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 10, 1), Time(0), 10),
                       BooleanFact("eid1", FeatureId("ns2", "fid3"), Date(2012, 3, 20), Time(0), true)))

    val directory: String = path(TempFiles.createTempDir("factdiff").getPath).pp
    val input1 = directory + "/1"
    val input2 = directory + "/2"
    val output = directory + "/out"
    val errors = directory + "/errors"

    persist(PartitionFactThriftStorageV1.PartitionedFactThriftStorer(input1, None).storeScoobi(facts1),
            PartitionFactThriftStorageV1.PartitionedFactThriftStorer(input2, None).storeScoobi(facts1))

    FactDiff.scoobiJob(input1, input2, output, errors).run(sc) must beOk

    Hdfs.readWith(new Path(output), is => Streams.read(is)).run(sc) must beOkValue("")
    Hdfs.readWith(new Path(errors), is => Streams.read(is)).run(sc) must beOkValue("")
  }

}
