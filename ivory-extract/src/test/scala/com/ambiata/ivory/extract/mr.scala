package com.ambiata.ivory.extract

import com.ambiata.ivory.core._

import org.specs2._
import org.scalacheck._, Arbitraries._
import org.apache.hadoop.io.BytesWritable


object SnapshotJobSpec extends Specification with ScalaCheck { def is = s2"""

SnapshotJobSpec
-----------

  populateKey mutates a BytesWritable with the correct bytes     $e1

"""

  val writable = new BytesWritable
  def e1 = prop((f: Fact) => {
    SnapshotJob.populateKey(f, writable)
    writable.copyBytes must_== (f.entity.getBytes ++ f.namespace.getBytes ++ f.feature.getBytes)
  })
}
