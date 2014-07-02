package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.saws.core._
import com.ambiata.saws.s3.S3

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.ScoobiS3Action._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.alien.hdfs.HdfsS3Action._
import com.ambiata.mundane.control._

trait IvoryLoader[A] {
  def load: A
}

trait IvoryStorer[A, B] {
  def store(a: A): B
}

trait IvoryScoobiLoader[A] {
  def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ A]
}

trait IvoryScoobiStorer[A, +B] {
  def storeScoobi(dlist: DList[A])(implicit sc: ScoobiConfiguration): B
  def storeMeta: ScoobiAction[Unit] =
    ScoobiAction.ok(())
}

/**
 * Fact loaders/storers
 */
case class InternalFactsetFactLoader(repo: HdfsRepository, factset: Factset, from: Option[Date], to: Option[Date]) {
  def load: ScoobiAction[DList[ParseError \/ Fact]] = for {
    sc <- ScoobiAction.scoobiConfiguration
    v  <- ScoobiAction.fromResultTIO(Versions.read(repo, factset))
    l   = IvoryStorage.factsetLoader(v, repo.factset(factset).toHdfs, from, to)
  } yield l.loadScoobi(sc)
}

case class InternalFactsetFactS3Loader(repo: S3Repository, factset: Factset, from: Option[Date], to: Option[Date]) {
  def load: ScoobiS3Action[DList[ParseError \/ Fact]] = for {
    sc <- ScoobiS3Action.scoobiConfiguration
    v  <- ScoobiS3Action.fromResultTIO(Versions.read(repo, factset))
  } yield IvoryStorage.factsetLoader(v, new Path("s3://"+repo.bucket+"/"+repo.factset(factset).path), from, to).loadScoobi(sc)
}

case class InternalFeatureStoreFactLoader(repo: HdfsRepository, store: FeatureStore, from: Option[Date], to: Option[Date]) {
  def load: ScoobiAction[DList[ParseError \/ (Priority, Factset, Fact)]] = for {
    sc       <- ScoobiAction.scoobiConfiguration
    factsets <- ScoobiAction.fromHdfs(store.factsets.traverse(factset => for {
                  ve <- Hdfs.exists(repo.version(factset.set).toHdfs)
                  s  <- Hdfs.size(repo.factset(factset.set).toHdfs)
                } yield (factset, ve && s != 0))).map(_.collect({ case (factset, true) => factset }))
    versions <- ScoobiAction.fromResultTIO(factsets.traverseU(factset =>
      Versions.read(repo, factset.set).map(factset -> _)))
    combined: List[(FactsetVersion, List[PrioritizedFactset])] = versions.groupBy(_._2).toList.map({ case (k, vs) => (k, vs.map(_._1)) })
    loaded = combined.map({ case (v, fss) => IvoryStorage.multiFactsetLoader(v, repo.factsets.toHdfs, fss, from, to).loadScoobi(sc) })
  } yield if(loaded.isEmpty) DList[ParseError \/ (Priority, Factset, Fact)]() else loaded.reduce(_++_)
}

case class InternalFactsetFactStorer(repo: HdfsRepository, factset: Factset, codec: Option[CompressionCodec]) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
  def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration) =
    IvoryStorage.factsetStorer(repo.factset(factset).path, codec).storeScoobi(dlist)
}

/**
 * Dictionary loaders/storers
 */
case class InternalDictionaryStorer(repo: HdfsRepository, name: String) extends IvoryStorer[Dictionary, Hdfs[Unit]] {
  import DictionaryTextStorage._
  def store(dict: Dictionary): Hdfs[Unit] =
    DictionaryTextStorer(repo.dictionaryByName(name).toHdfs).store(dict)
}

/**
 * Feature store loaders/storers
 */
case class InternalFeatureStoreLoader(repo: HdfsRepository, name: String) extends IvoryLoader[Hdfs[FeatureStore]] {
  import FeatureStoreTextStorage._
  def load: Hdfs[FeatureStore] =
    FeatureStoreTextLoader(repo.storeByName(name).toHdfs).load
}

case class InternalFeatureStoreLoaderS3(repository: S3Repository, name: String) {
  import FeatureStoreTextStorage._
  def load: HdfsS3Action[FeatureStore] = for {
    file  <- HdfsS3Action.fromAction(S3.downloadFile(repository.bucket, repository.storeByName(name).path, to = (repository.tmp </> name).path))
    store <- HdfsS3Action.fromHdfs(FeatureStoreTextLoader(new Path(file.getPath)).load)
  } yield store
}

case class InternalFeatureStoreStorer(repo: HdfsRepository, name: String) extends IvoryStorer[FeatureStore, Hdfs[Unit]] {
  import FeatureStoreTextStorage._
  def store(store: FeatureStore): Hdfs[Unit] =
    FeatureStoreTextStorer(repo.storeByName(name).toHdfs).store(store)
}

case class InternalFeatureStoreStorerS3(repository: S3Repository, name: String) {
  import FeatureStoreTextStorage._
  def store(store: FeatureStore): HdfsS3Action[Unit] = {
    val tmpPath = new Path(repository.tmp.path, repository.storeByName(name).path)
    for {
      _ <- HdfsS3Action.fromHdfs(FeatureStoreTextStorer(tmpPath).store(store))
      _ <- HdfsS3.putPaths(repository.bucket, repository.storeByName(name).path, tmpPath, glob = "*")
      _ <- HdfsS3Action.fromHdfs(Hdfs.filesystem.map(fs => fs.delete(tmpPath, true)))
    } yield ()
  }
}

object IvoryStorage {

  // this is the version that factsets are written as
  val factsetVersion = FactsetVersionTwo
  def factsetStorer(path: String, codec: Option[CompressionCodec]): IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] =
    PartitionFactThriftStorageV2.PartitionedFactThriftStorer(path, codec)

  /**
   * Get the loader for a given version
   */
  def factsetLoader(version: FactsetVersion, path: Path, from: Option[Date], to: Option[Date]): IvoryScoobiLoader[Fact] = version match {
    case FactsetVersionOne => PartitionFactThriftStorageV1.PartitionedFactThriftLoader(List(path.toString), from, to)
    case FactsetVersionTwo => PartitionFactThriftStorageV2.PartitionedFactThriftLoader(List(path.toString), from, to)
  }

  def multiFactsetLoader(version: FactsetVersion, path: Path, factsets: List[PrioritizedFactset], from: Option[Date], to: Option[Date]): IvoryScoobiLoader[(Priority, Factset, Fact)] = version match {
    case FactsetVersionOne => PartitionFactThriftStorageV1.PartitionedMultiFactsetThriftLoader(path.toString, factsets, from, to)
    case FactsetVersionTwo => PartitionFactThriftStorageV2.PartitionedMultiFactsetThriftLoader(path.toString, factsets, from, to)
  }

  def writeFactsetVersion(repo: HdfsRepository, factsets: List[Factset]): Hdfs[Unit] =
    Hdfs.fromResultTIO(Versions.writeAll(repo, factsets, factsetVersion))

  implicit class IvoryFactStorage(dlist: DList[Fact]) {
    def toIvoryFactset(repo: HdfsRepository, factset: Factset, codec: Option[CompressionCodec])(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] =
      InternalFactsetFactStorer(repo, factset, codec).storeScoobi(dlist)(sc)
  }

  /* Facts */
  def factsFromIvoryStore(repo: HdfsRepository, store: FeatureStore): ScoobiAction[DList[ParseError \/ (Priority, Factset, Fact)]] =
    InternalFeatureStoreFactLoader(repo, store, None, None).load

  def factsFromIvoryStoreFrom(repo: HdfsRepository, store: FeatureStore, from: Date): ScoobiAction[DList[ParseError \/ (Priority, Factset, Fact)]] =
    InternalFeatureStoreFactLoader(repo, store, Some(from), None).load

  def factsFromIvoryStoreTo(repo: HdfsRepository, store: FeatureStore, to: Date): ScoobiAction[DList[ParseError \/ (Priority, Factset, Fact)]] =
    InternalFeatureStoreFactLoader(repo, store, None, Some(to)).load

  def factsFromIvoryStoreBetween(repo: HdfsRepository, store: FeatureStore, from: Date, to: Date): ScoobiAction[DList[ParseError \/ (Priority, Factset, Fact)]] =
    InternalFeatureStoreFactLoader(repo, store, Some(from), Some(to)).load

  def factsFromIvoryFactset(repo: HdfsRepository, factset: Factset): ScoobiAction[DList[ParseError \/ Fact]] =
    InternalFactsetFactLoader(repo, factset, None, None).load

  def factsFromIvoryFactsetFrom(repo: HdfsRepository, factset: Factset, from: Date): ScoobiAction[DList[ParseError \/ Fact]] =
    InternalFactsetFactLoader(repo, factset, Some(from), None).load

  def factsFromIvoryFactsetTo(repo: HdfsRepository, factset: Factset, to: Date): ScoobiAction[DList[ParseError \/ Fact]] =
    InternalFactsetFactLoader(repo, factset, None, Some(to)).load

  def factsFromIvoryFactsetBetween(repo: HdfsRepository, factset: Factset, from: Date, to: Date): ScoobiAction[DList[ParseError \/ Fact]] =
    InternalFactsetFactLoader(repo, factset, Some(from), Some(to)).load

  def factsFromIvoryS3Factset(repository: S3Repository, factset: Factset): ScoobiS3Action[DList[ParseError \/ Fact]] =
    InternalFactsetFactS3Loader(repository, factset, None, None).load

  def factsFromIvoryS3FactsetFrom(repository: S3Repository, factset: Factset, from: Date): ScoobiS3Action[DList[ParseError \/ Fact]] =
    InternalFactsetFactS3Loader(repository, factset, Some(from), None).load

  def factsFromIvoryS3FactsetTo(repository: S3Repository, factset: Factset, to: Date): ScoobiS3Action[DList[ParseError \/ Fact]] =
    InternalFactsetFactS3Loader(repository, factset, None, Some(to)).load

  def factsFromIvoryS3FactsetBetween(repository: S3Repository, factset: Factset, from: Date, to: Date): ScoobiS3Action[DList[ParseError \/ Fact]] =
    InternalFactsetFactS3Loader(repository, factset, Some(from), Some(to)).load

  def dictionaryFromIvory(repo: Repository): ResultTIO[Dictionary] =
    DictionaryThriftStorage(repo).load

  def dictionaryToIvory(repo: Repository, dictionary: Dictionary): ResultTIO[Unit] =
    DictionaryThriftStorage(repo).store(dictionary).map(_ => ())

  /* Store */
  def storeFromIvory(repo: HdfsRepository, name: String): Hdfs[FeatureStore] =
    InternalFeatureStoreLoader(repo, name).load

  def storeFromIvoryS3(repository: S3Repository, name: String): HdfsS3Action[FeatureStore] =
    InternalFeatureStoreLoaderS3(repository, name).load

  def storeToIvory(repo: HdfsRepository, store: FeatureStore, name: String): Hdfs[Unit] =
    InternalFeatureStoreStorer(repo, name).store(store)

  def storeToIvoryS3(repository: S3Repository, store: FeatureStore, name: String): HdfsS3Action[Unit] =
    InternalFeatureStoreStorerS3(repository, name).store(store)
}
