package com.ambiata.ivory.scoobi

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.scoobi.WireFormats.ShortWireFormat
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

  implicit def FactsetWireFormat: WireFormat[Factset] =
    implicitly[WireFormat[String]].xmap(Factset, _.name)

  implicit def PriorityWireFormat: WireFormat[Priority] =
    implicitly[WireFormat[Short]].xmap(Priority.unsafe, _.toShort)

}
