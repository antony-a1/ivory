package com.ambiata.ivory.scoobi

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.nicta.scoobi._, Scoobi._

object FactFormats {
  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  implicit def FactWireFormat: WireFormat[Fact] = WireFormats.factWireFormat
  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  implicit def FactSeqSchema: SeqSchema[Fact] = SeqSchemas.factSeqSchema
  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  implicit def ThriftFactWireFormat: WireFormat[ThriftFact] = WireFormats.thriftFactWireFormat
  /* WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  implicit def ThriftFactSeqSchema: SeqSchema[ThriftFact] = SeqSchemas.thriftFactSeqSchema
}
