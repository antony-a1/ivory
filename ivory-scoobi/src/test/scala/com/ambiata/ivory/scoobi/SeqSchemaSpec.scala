package com.ambiata.ivory.scoobi

import org.specs2._
import java.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import SeqSchemas._

import scala.collection.JavaConverters._

class SeqSchemaSpec extends Specification { def is = s2"""
  Can convert ThriftFact to/from writable using SeqSchema     $e1
                                                              """
  def e1 = {
    val expected = List(new ThriftFact("eid1", "fid1", ThriftFactValue.s("abc")),
                        new ThriftFact("eid2", "fid2", ThriftFactValue.b(true)).setSeconds(123),
                        new ThriftFact("eid3", "fid3", ThriftFactValue.i(123)).setSeconds(987),
                        new ThriftFact("eid4", "fid4", ThriftFactValue.l(123l)),
                        new ThriftFact("eid5", "fid5", ThriftFactValue.d(1.0)),
                        new ThriftFact("eid6", "fid6", ThriftFactValue.t(new ThriftTombstone)))
    val sch = mkThriftSchema(new ThriftFact())
    expected.map(tf => sch.fromWritable(sch.toWritable(tf)) must_== tf)
  }
}

