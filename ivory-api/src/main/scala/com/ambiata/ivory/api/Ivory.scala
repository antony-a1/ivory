package com.ambiata.ivory.api

object Ivory
  extends com.ambiata.ivory.core.IvorySyntax
  with com.ambiata.ivory.scoobi.WireFormats
  with com.ambiata.ivory.scoobi.FactFormats {

  /**
   * Core ivory data types.
   */
  type Fact = com.ambiata.ivory.core.Fact
  val Fact = com.ambiata.ivory.core.Fact

  type Value = com.ambiata.ivory.core.Value
  val BooleanValue = com.ambiata.ivory.core.BooleanValue
  val IntValue = com.ambiata.ivory.core.IntValue
  val LongValue = com.ambiata.ivory.core.LongValue
  val DoubleValue = com.ambiata.ivory.core.DoubleValue
  val StringValue = com.ambiata.ivory.core.StringValue
  val TombstoneValue = com.ambiata.ivory.core.TombstoneValue

  type Factset = com.ambiata.ivory.core.Factset
  val Factset = com.ambiata.ivory.core.Factset

  type Priority = com.ambiata.ivory.core.Priority
  val Priority = com.ambiata.ivory.core.Priority

  type Dictionary = com.ambiata.ivory.core.Dictionary
  val Dictionary = com.ambiata.ivory.core.Dictionary

  type FeatureId = com.ambiata.ivory.core.FeatureId
  val FeatureId = com.ambiata.ivory.core.FeatureId

  type FeatureMeta = com.ambiata.ivory.core.FeatureMeta
  val FeatureMeta = com.ambiata.ivory.core.FeatureMeta

  type Encoding = com.ambiata.ivory.core.Encoding
  val BooleanEncoding = com.ambiata.ivory.core.BooleanEncoding
  val IntEncoding = com.ambiata.ivory.core.IntEncoding
  val LongEncoding = com.ambiata.ivory.core.LongEncoding
  val DoubleEncoding = com.ambiata.ivory.core.DoubleEncoding
  val StringEncoding = com.ambiata.ivory.core.StringEncoding

  // FIX rename???
  type Type = com.ambiata.ivory.core.Type
  val NumericalType = com.ambiata.ivory.core.NumericalType
  val ContinuousType = com.ambiata.ivory.core.ContinuousType
  val CategoricalType = com.ambiata.ivory.core.CategoricalType
  val BinaryType = com.ambiata.ivory.core.BinaryType

  type FeatureStore = com.ambiata.ivory.core.FeatureStore
  val FeatureStore = com.ambiata.ivory.core.FeatureStore

  type Date = com.ambiata.ivory.core.Date
  val Date = com.ambiata.ivory.core.Date

  type DateTime = com.ambiata.ivory.core.DateTime
  val DateTime = com.ambiata.ivory.core.DateTime

  type Time = com.ambiata.ivory.core.Time
  val Time = com.ambiata.ivory.core.Time

  type ParseError = com.ambiata.ivory.core.ParseError
  val ParseError = com.ambiata.ivory.core.ParseError

  type Partition = com.ambiata.ivory.core.Partition
  val Partition = com.ambiata.ivory.core.Partition

  /**
   * Debug
   */
  val printErrors = com.ambiata.ivory.extract.print.PrintErrors.print _
  // MTH rename: printSnapshots
  val printFacts = com.ambiata.ivory.extract.print.PrintFacts.print _
  // MTH rename: printFactsets
  val printInternalFacts = com.ambiata.ivory.extract.print.PrintInternalFacts.print _
}
