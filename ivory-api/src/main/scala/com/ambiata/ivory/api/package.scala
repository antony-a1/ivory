package com.ambiata.ivory

package object api {

  /**
   * Core types
   */
  type Fact = com.ambiata.ivory.core.Fact

  type Value = com.ambiata.ivory.core.Value
  type BooleanValue = com.ambiata.ivory.core.BooleanValue
  type IntValue = com.ambiata.ivory.core.IntValue
  type LongValue = com.ambiata.ivory.core.LongValue
  type DoubleValue = com.ambiata.ivory.core.DoubleValue
  type StringValue = com.ambiata.ivory.core.StringValue
  type TombstoneValue = com.ambiata.ivory.core.TombstoneValue

  type Factset = com.ambiata.ivory.core.Factset
  type PrioritizedFactset = com.ambiata.ivory.core.PrioritizedFactset
  type Priority = com.ambiata.ivory.core.Priority

  type Dictionary = com.ambiata.ivory.core.Dictionary
  type FeatureId = com.ambiata.ivory.core.FeatureId
  type FeatureMeta = com.ambiata.ivory.core.FeatureMeta

  type Encoding = com.ambiata.ivory.core.Encoding
  type Type = com.ambiata.ivory.core.Type

  type FeatureStore = com.ambiata.ivory.core.FeatureStore

  type Date = com.ambiata.ivory.core.Date
  type DateTime = com.ambiata.ivory.core.DateTime
  type Time = com.ambiata.ivory.core.Time

  type ParseError = com.ambiata.ivory.core.ParseError

  type Partition = com.ambiata.ivory.core.Partition

  /**
   * Repository types
   */
  type Repository = com.ambiata.ivory.storage.repository.Repository
  type HdfsRepository = com.ambiata.ivory.storage.repository.HdfsRepository
  type S3Repository = com.ambiata.ivory.storage.repository.S3Repository
  type LocalRepository = com.ambiata.ivory.storage.repository.LocalRepository

  /**
   * Scoobi types
   */
  type ScoobiAction[A] = com.ambiata.ivory.scoobi.ScoobiAction[A]

  /**
   * Storage types
   */
  type IvoryLoader[A] = com.ambiata.ivory.storage.legacy.IvoryLoader[A]
  type IvoryStorer[A, B] = com.ambiata.ivory.storage.legacy.IvoryStorer[A, B]
  type IvoryScoobiLoader[A] = com.ambiata.ivory.storage.legacy.IvoryScoobiLoader[A]
  type IvoryScoobiStorer[A, +B] = com.ambiata.ivory.storage.legacy.IvoryScoobiStorer[A, B]

  /**
   * Core
   */
  val Fact = com.ambiata.ivory.core.Fact
  val BooleanFact = com.ambiata.ivory.core.BooleanFact
  val IntFact = com.ambiata.ivory.core.IntFact
  val LongFact = com.ambiata.ivory.core.LongFact
  val DoubleFact = com.ambiata.ivory.core.DoubleFact
  val StringFact = com.ambiata.ivory.core.StringFact
  val TombstoneFact = com.ambiata.ivory.core.TombstoneFact

  val BooleanValue = com.ambiata.ivory.core.BooleanValue
  val IntValue = com.ambiata.ivory.core.IntValue
  val LongValue = com.ambiata.ivory.core.LongValue
  val DoubleValue = com.ambiata.ivory.core.DoubleValue
  val StringValue = com.ambiata.ivory.core.StringValue
  val TombstoneValue = com.ambiata.ivory.core.TombstoneValue

  val Factset = com.ambiata.ivory.core.Factset
  val PrioritizedFactset = com.ambiata.ivory.core.PrioritizedFactset
  val Priority = com.ambiata.ivory.core.Priority

  val Dictionary = com.ambiata.ivory.core.Dictionary
  val FeatureId = com.ambiata.ivory.core.FeatureId
  val FeatureMeta = com.ambiata.ivory.core.FeatureMeta

  val BooleanEncoding = com.ambiata.ivory.core.BooleanEncoding
  val IntEncoding = com.ambiata.ivory.core.IntEncoding
  val LongEncoding = com.ambiata.ivory.core.LongEncoding
  val DoubleEncoding = com.ambiata.ivory.core.DoubleEncoding
  val StringEncoding = com.ambiata.ivory.core.StringEncoding

  val NumericalType = com.ambiata.ivory.core.NumericalType
  val ContinuousType = com.ambiata.ivory.core.ContinuousType
  val CategoricalType = com.ambiata.ivory.core.CategoricalType
  val BinaryType = com.ambiata.ivory.core.BinaryType

  val FeatureStore = com.ambiata.ivory.core.FeatureStore

  val Date = com.ambiata.ivory.core.Date
  val DateTime = com.ambiata.ivory.core.DateTime
  val Time = com.ambiata.ivory.core.Time

  val ParseError = com.ambiata.ivory.core.ParseError

  val Partition = com.ambiata.ivory.core.Partition

  val ThriftSerialiser = com.ambiata.ivory.core.thrift.ThriftSerialiser

  /**
   * Storage
   */
  val writeFactsetVersion = com.ambiata.ivory.storage.legacy.IvoryStorage.writeFactsetVersion _

  val factsFromIvoryStore = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryStore _
  val factsFromIvoryStoreFrom = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryStoreFrom _
  val factsFromIvoryStoreTo = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryStoreTo _
  val factsFromIvoryStoreBetween = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryStoreBetween _
  val factsFromIvoryFactset = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryFactset _
  val factsFromIvoryFactsetFrom = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryFactsetFrom _
  val factsFromIvoryFactsetTo = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryFactsetTo _
  val factsFromIvoryFactsetBetween = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryFactsetBetween _
  val factsFromIvoryS3Factset = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryS3Factset _
  val factsFromIvoryS3FactsetFrom = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryS3FactsetFrom _
  val factsFromIvoryS3FactsetTo = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryS3FactsetTo _
  val factsFromIvoryS3FactsetBetween = com.ambiata.ivory.storage.legacy.IvoryStorage.factsFromIvoryS3FactsetBetween _

  val dictionaryFromHdfs = com.ambiata.ivory.storage.legacy.DictionaryTextStorage.dictionaryFromHdfs _
  val dictionaryToHdfs = com.ambiata.ivory.storage.legacy.DictionaryTextStorage.dictionaryToHdfs _
  val dictionaryFromInputStream = com.ambiata.ivory.storage.legacy.DictionaryTextStorage.fromInputStream _
  val dictionaryFromIvory = com.ambiata.ivory.storage.legacy.IvoryStorage.dictionaryFromIvory _
  val dictionariesToIvory = com.ambiata.ivory.storage.legacy.IvoryStorage.dictionariesToIvory _
  val dictionaryToIvory = com.ambiata.ivory.storage.legacy.IvoryStorage.dictionaryToIvory _
  val dictionaryToIvoryS3 = com.ambiata.ivory.storage.legacy.IvoryStorage.dictionaryToIvoryS3 _
  val dictionariesToIvoryS3 = com.ambiata.ivory.storage.legacy.IvoryStorage.dictionariesToIvoryS3 _

  val storeFromIvory = com.ambiata.ivory.storage.legacy.IvoryStorage.storeFromIvory _
  val storeToIvory = com.ambiata.ivory.storage.legacy.IvoryStorage.storeToIvory _
  val storeFromIvoryS3 = com.ambiata.ivory.storage.legacy.IvoryStorage.storeFromIvoryS3 _
  val storeToIvoryS3 = com.ambiata.ivory.storage.legacy.IvoryStorage.storeToIvoryS3 _

  /**
   * Repository
   */
  val Repository = com.ambiata.ivory.storage.repository.Repository
  val createRepository = com.ambiata.ivory.storage.legacy.CreateRepository.onHdfs _
  val createFeatureStore = com.ambiata.ivory.storage.legacy.CreateFeatureStore.onHdfs _

  /**
   * Ingest
   */
  val importWorkflow = com.ambiata.ivory.storage.legacy.fatrepo.ImportWorkflow.onHdfs _
  val importDictionary = com.ambiata.ivory.ingest.DictionaryImporter.onHdfs _
  val importStore = com.ambiata.ivory.ingest.FeatureStoreImporter.onHdfs _
  val importDictionaryS3 = com.ambiata.ivory.ingest.DictionaryImporter.onS3 _
  val importStoreS3 = com.ambiata.ivory.ingest.FeatureStoreImporter.onS3 _
  implicit def dlist2IvoryFactStorage(dlist: com.nicta.scoobi.core.DList[Fact]): com.ambiata.ivory.storage.legacy.IvoryStorage.IvoryFactStorage =
    com.ambiata.ivory.storage.legacy.IvoryStorage.IvoryFactStorage(dlist)

  /**
   * Extract
   */
  val takeSnapshot = com.ambiata.ivory.extract.HdfsSnapshot.takeSnapshot _
  val extractChord = com.ambiata.ivory.extract.Chord.onHdfs _
  val pivot = com.ambiata.ivory.extract.Pivot.onHdfs _

  /**
   * Validate
   */
  val validateStore = com.ambiata.ivory.validate.Validate.validateHdfsStore _
  val validateFactSet = com.ambiata.ivory.validate.Validate.validateHdfsFactSet _
  val validateFact = com.ambiata.ivory.validate.Validate.validateFact _
  val validateEncoding = com.ambiata.ivory.validate.Validate.validateEncoding _

  /**
   * Scoobi
   */
  val ScoobiAction = com.ambiata.ivory.scoobi.ScoobiAction
  val Groupings = com.ambiata.ivory.scoobi.Groupings

  /**
   * Debug
   */
  val printErrors = com.ambiata.ivory.extract.print.PrintErrors.print _
  val printFacts = com.ambiata.ivory.extract.print.PrintFacts.print _
  val printInternalFacts = com.ambiata.ivory.extract.print.PrintInternalFacts.print _
}
