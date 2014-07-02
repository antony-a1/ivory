package com.ambiata.ivory.api

/**
 * The ivory "retire" API forms an exported API for "deprecated",
 * "legacy" or "dangerous" compontents. The goal of these APIs is
 * to eventually phase them out, and replace them with better,
 * stable APIs.
 */
object IvoryRetire {
  /* Some ivory API's require currently force explicit use of scoobi,
     this component is generally how those API's are exposed, however
     in the future we will move to more general, implementation
     neutral APIs. */
  type ScoobiAction[A] = com.ambiata.ivory.scoobi.ScoobiAction[A]
  val ScoobiAction = com.ambiata.ivory.scoobi.ScoobiAction
  type HdfsRepository = com.ambiata.ivory.storage.repository.HdfsRepository

  /**
   * Storage types. These components expose the internal representations of ivory.
   * They are likely to be highly volatile, and will be changing in the near future.
   * They will be replaced by a safer, stable API that lets users interact with
   * ivory, without concern for the current implementation.
   */
  type IvoryLoader[A] = com.ambiata.ivory.storage.legacy.IvoryLoader[A]
  type IvoryStorer[A, B] = com.ambiata.ivory.storage.legacy.IvoryStorer[A, B]
  type IvoryScoobiLoader[A] = com.ambiata.ivory.storage.legacy.IvoryScoobiLoader[A]
  type IvoryScoobiStorer[A, +B] = com.ambiata.ivory.storage.legacy.IvoryScoobiStorer[A, B]

  val writeFactsetVersion = com.ambiata.ivory.storage.legacy.IvoryStorage.writeFactsetVersion _

  val snapshotFromHdfs = com.ambiata.ivory.storage.legacy.SnapshotStorageV1.snapshotFromHdfs _
  val snapshotToHdfs = com.ambiata.ivory.storage.legacy.SnapshotStorageV1.snapshotToHdfs _

  val createRepository = com.ambiata.ivory.storage.legacy.CreateRepository.onHdfs _

  /**
   * Ingest types. These components expose the internal representations of ivory.
   * They are likely to be highly volatile, and will be changing in the near future.
   * They will be replaced by a safer, stable API that lets users interact with
   * ivory, without concern for the current implementation.
   */
  val importWorkflow = com.ambiata.ivory.storage.legacy.fatrepo.ImportWorkflow.onHdfs _
  val importDictionary = com.ambiata.ivory.ingest.DictionaryImporter.fromPath _
  val importStore = com.ambiata.ivory.ingest.FeatureStoreImporter.onHdfs _

  implicit def DListToIvoryFactStorage(dlist: com.nicta.scoobi.core.DList[Ivory.Fact]): com.ambiata.ivory.storage.legacy.IvoryStorage.IvoryFactStorage =
    com.ambiata.ivory.storage.legacy.IvoryStorage.IvoryFactStorage(dlist)

  /**
   * Extract types. These components expose the internal representations of ivory.
   * They are likely to be highly volatile, and will be changing in the near future.
   * They will be replaced by a safer, stable API that lets users interact with
   * ivory, without concern for the current implementation.
   */
  val snapshot = com.ambiata.ivory.extract.HdfsSnapshot.snapshot _
  val extractChord = com.ambiata.ivory.extract.Chord.onHdfs _
  val pivot = com.ambiata.ivory.extract.Pivot.onHdfs _

  /**
   * Validate, these APIs are too low level, and will be replaced by more
   * general validation mechanisms.
   */
  val validateStore = com.ambiata.ivory.validate.Validate.validateHdfsStore _
  val validateFactSet = com.ambiata.ivory.validate.Validate.validateHdfsFactSet _
  val validateFact = com.ambiata.ivory.validate.Validate.validateFact _
  val validateEncoding = com.ambiata.ivory.validate.Validate.validateEncoding _
}
