package com.ambiata.ivory.storage

import scalaz.{DList => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.mutable._
import com.nicta.scoobi.testing.SimpleJobs
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles

import org.specs2._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.WireFormats, WireFormats._

class DenseRowTextStorageSpec extends HadoopSpecification with SimpleJobs {
  override def isCluster = false

  "Dense rows line up" >> {
    val features = List((0, FeatureId("ns1", "fid1"), FeatureMeta(StringEncoding, CategoricalType, "")),
                        (1, FeatureId("ns1", "fid2"), FeatureMeta(IntEncoding, ContinuousType, "")),
                        (2, FeatureId("ns1", "fid3"), FeatureMeta(BooleanEncoding, CategoricalType, "")),
                        (3, FeatureId("ns1", "fid4"), FeatureMeta(DoubleEncoding, NumericalType, "")))
    val facts = List(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 1, 1), Time(0), "abc"),
                     IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 1, 1), Time(0), 123),
                     BooleanFact("eid1", FeatureId("ns1", "fid3"), Date(2012, 1, 1), Time(0), true))

    DenseRowTextStorageV1.makeDense(facts, features, "☠") must_== List("abc", "123", "true", "☠")
  }

  "Dense rows stored correctly" >> { implicit sc: ScoobiConfiguration =>
    implicit val FactWireFormat = WireFormats.FactWireFormat

    val directory = path(TempFiles.createTempDir("denserowtextstorer").getPath)

    val dict = Dictionary("dict", Map(FeatureId("ns1", "fid1") -> FeatureMeta(StringEncoding, CategoricalType, ""),
                                      FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding, ContinuousType, ""),
                                      FeatureId("ns1", "fid3") -> FeatureMeta(BooleanEncoding, CategoricalType, ""),
                                      FeatureId("ns1", "fid4") -> FeatureMeta(DoubleEncoding, NumericalType, "")))

    val facts = fromLazySeq(
                  Seq(BooleanFact("eid1", FeatureId("ns1", "fid3"), Date(2012, 1, 1), Time(0), true),
                      StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 1, 1), Time(0), "abc"),
                      IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 1, 1), Time(0), 123),
                      DoubleFact("eid2", FeatureId("ns1", "fid4"), Date(2012, 2, 2), Time(123), 2.0),
                      IntFact("eid2", FeatureId("ns1", "fid2"), Date(2012, 3, 1), Time(0), 9)))

    val res = DenseRowTextStorageV1.DenseRowTextStorer(directory, dict).storeScoobi(facts).run.toList
    res must_== List("eid1|abc|123|true|NA", "eid2|NA|9|NA|2.0")
  }
}
