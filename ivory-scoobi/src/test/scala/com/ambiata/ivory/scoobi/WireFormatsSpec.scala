package com.ambiata.ivory.scoobi

import org.specs2._
import java.io._
import org.joda.time.LocalDate

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import WireFormats._

import scala.collection.JavaConverters._

class WireFormatsSpec extends Specification { def is = s2"""
  Can serialise/deserialise thrift             $e1
  Can serialise/deserialise LocalDate          $e2
  Can serialise/desrialise Facts               $e3
                                               """
  def e1 = {
    val expected = List(new ThriftFact("eid1", "fid1", ThriftFactValue.s("abc")),
                        new ThriftFact("eid2", "fid2", ThriftFactValue.b(true)).setSeconds(123),
                        new ThriftFact("eid3", "fid3", ThriftFactValue.i(123)).setSeconds(987),
                        new ThriftFact("eid4", "fid4", ThriftFactValue.l(123l)),
                        new ThriftFact("eid5", "fid5", ThriftFactValue.d(1.0)),
                        new ThriftFact("eid6", "fid6", ThriftFactValue.t(new ThriftTombstone)))
    val fmt = mkThriftFmt(new ThriftFact())
    expected.map(tf => {
      val bos = new ByteArrayOutputStream(2048)
      val out = new DataOutputStream(bos)
      fmt.toWire(tf, out)
      out.flush()

      val actual = fmt.fromWire(new DataInputStream(new ByteArrayInputStream(bos.toByteArray)))
      actual must_== tf
    })
  }

  def e2 = {
    val date = new LocalDate(2012, 1, 30)

    val bos = new ByteArrayOutputStream
    val out = new DataOutputStream(bos)
    LocalDateFmt.toWire(date, out)
    out.flush()

    val actual = LocalDateFmt.fromWire(new DataInputStream(new ByteArrayInputStream(bos.toByteArray)))
    actual.toString("yyyy-MM-dd") must_== date.toString("yyyy-MM-dd")
  }

  def e3 = {
    val expected = List(StringFact("eid1", FeatureId("ns1", "nm1"), Date(2012, 1, 30), 0, "value1"),
                        IntFact("eid1", FeatureId("ns1", "nm2"), Date(2012, 1, 30), 123, 9),
                        LongFact("eid2", FeatureId("ns1", "nm4"), Date(2012, 2, 20), 0, 456l),
                        BooleanFact("eid2", FeatureId("ns1", "nm3"), Date(2012, 2, 1), 1, true),
                        DoubleFact("eid3", FeatureId("ns1", "nm4"), Date(2012, 3, 1), 0, 3.2),
                        TombstoneFact("eid3", FeatureId("ns1", "nm5"), Date(2012, 3, 15), 987))

    val bos = new ByteArrayOutputStream
    val out = new DataOutputStream(bos)
    expected.map(f => {
      bos.reset()
      FactFmt.toWire(f, out)
      out.flush()

      val actual = FactFmt.fromWire(new DataInputStream(new ByteArrayInputStream(bos.toByteArray)))
      actual must_== f
    })
  }
}
