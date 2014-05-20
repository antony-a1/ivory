package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
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
    versions <- ScoobiAction.fromResultTIO(store.factsets.traverseU(factset =>
      Versions.read(repo, factset.set).map(factset -> _)))
    combined: List[(FactsetVersion, List[PrioritizedFactset])] = versions.groupBy(_._2).toList.map({ case (k, vs) => (k, vs.map(_._1)) })
    loaded = combined.map({ case (v, fss) => IvoryStorage.multiFactsetLoader(v, repo.factsets.toHdfs, fss, from, to).loadScoobi(sc) })
  } yield if(loaded.isEmpty) DList[ParseError \/ (Priority, Factset, Fact)]() else loaded.reduce(_++_)
}

case class InternalFactsetFactStorer(repo: HdfsRepository, factset: Factset) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
  def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration) =
    IvoryStorage.factsetStorer(repo.factset(factset).path).storeScoobi(dlist)
}

/**
 * Dictionary loaders/storers
 */
case class InternalDictionaryLoader(repo: HdfsRepository, name: String) extends IvoryLoader[Hdfs[Dictionary]] {
  import DictionaryTextStorage._
  def load: Hdfs[Dictionary] =
    DictionaryTextLoader(repo.dictionaryByName(name).toHdfs).load
}

case class InternalDictionaryStorer(repo: HdfsRepository, name: String) extends IvoryStorer[Dictionary, Hdfs[Unit]] {
  import DictionaryTextStorage._
  def store(dict: Dictionary): Hdfs[Unit] =
    DictionaryTextStorer(repo.dictionaryByName(name).toHdfs).store(dict)
}

case class InternalDictionariesStorer(repo: HdfsRepository, name: String) extends IvoryStorer[List[Dictionary], Hdfs[Unit]] {
  import DictionaryTextStorage._
  def store(dicts: List[Dictionary]): Hdfs[Unit] =
    dicts.traverse(d => DictionaryTextStorer((repo.dictionaryByName(name) </> d.name).toHdfs).store(d)).map(_ => ())
}

case class DictionariesS3Storer(repository: S3Repository) {
  import DictionaryTextStorage._

  def store(dictionary: Dictionary, name: String): HdfsS3Action[Unit] = {
    val tmpPath = (repository.tmp </> repository.dictionaryByName(name)).toHdfs
    for {
      _ <- HdfsS3Action.fromHdfs(DictionaryTextStorer(tmpPath).store(dictionary))
      a <- HdfsS3.putPaths(repository.bucket, repository.dictionaryByName(name).path, new Path(tmpPath, "*"))
      _ <- HdfsS3Action.fromHdfs(Hdfs.filesystem.map(fs => fs.delete(tmpPath, true)))
    } yield a
  }

  def store(dictionaries: List[Dictionary], name: String): HdfsS3Action[Unit] =
    for {
      _ <- HdfsS3Action.fromHdfs(Hdfs.filesystem.map(fs => fs.mkdirs(repository.tmp.toHdfs)))
      _ <- HdfsS3Action.fromHdfs(dictionaries.traverse(d => DictionaryTextStorer((repository.tmp </> repository.dictionaryByName(name) </> d.name).toHdfs).store(d)))
      a <- HdfsS3.putPaths(repository.bucket, repository.dictionaryByName(name).path, (repository.tmp </> repository.dictionaryByName(name)).toHdfs, "*")
      _ <- HdfsS3Action.fromHdfs(Hdfs.filesystem.map(fs => fs.delete(repository.tmp.toHdfs, true)))
    } yield a
}

case class DictionariesS3Loader(repository: S3Repository) {
  import DictionaryTextStorage._

  def load(dictionaryName: String): HdfsS3Action[Dictionary] = for {
    _ <- HdfsS3Action.fromAction(S3.downloadFile(repository.bucket, repository.dictionaryByName(dictionaryName).path, (repository.tmp </> dictionaryName).path))
    d <- HdfsS3Action.fromHdfs(DictionaryTextLoader((repository.tmp </> dictionaryName).toHdfs).load)
  } yield d
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
  def factsetStorer(path: String): IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] =
    PartitionFactThriftStorageV2.PartitionedFactThriftStorer(path)

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
    def toIvoryFactset(repo: HdfsRepository, factset: Factset)(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] =
      InternalFactsetFactStorer(repo, factset).storeScoobi(dlist)(sc)
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

  def factsFromIvoryFactset(repository: S3Repository, factset: Factset): ScoobiS3Action[DList[ParseError \/ Fact]] =
    InternalFactsetFactS3Loader(repository, factset, None, None).load

  def factsFromIvoryFactsetFrom(repository: S3Repository, factset: Factset, from: Date): ScoobiS3Action[DList[ParseError \/ Fact]] =
    InternalFactsetFactS3Loader(repository, factset, Some(from), None).load

  def factsFromIvoryFactsetTo(repository: S3Repository, factset: Factset, to: Date): ScoobiS3Action[DList[ParseError \/ Fact]] =
    InternalFactsetFactS3Loader(repository, factset, None, Some(to)).load

  def factsFromIvoryFactsetBetween(repository: S3Repository, factset: Factset, from: Date, to: Date): ScoobiS3Action[DList[ParseError \/ Fact]] =
    InternalFactsetFactS3Loader(repository, factset, Some(from), Some(to)).load

  /* Dictionary */
  def dictionaryFromIvory(repo: HdfsRepository, name: String): Hdfs[Dictionary] =
    InternalDictionaryLoader(repo, name).load

  def dictionariesToIvory(repo: HdfsRepository, dicts: List[Dictionary], name: String): Hdfs[Unit] =
    InternalDictionariesStorer(repo, name).store(dicts)

  def dictionaryToIvory(repo: HdfsRepository, dict: Dictionary, name: String): Hdfs[Unit] =
    InternalDictionaryStorer(repo, name).store(dict)

  def dictionaryToIvory(repo: S3Repository, dict: Dictionary, name: String): HdfsS3Action[Unit] =
    DictionariesS3Storer(repo).store(dict, name)

  def dictionariesToIvory(repo: S3Repository, dictionaries: List[Dictionary], name: String): HdfsS3Action[Unit] =
    DictionariesS3Storer(repo).store(dictionaries, name)


  /* Store */
  def storeFromIvory(repo: HdfsRepository, name: String): Hdfs[FeatureStore] =
    InternalFeatureStoreLoader(repo, name).load

  def storeFromIvory(repository: S3Repository, name: String): HdfsS3Action[FeatureStore] =
    InternalFeatureStoreLoaderS3(repository, name).load

  def storeToIvory(repo: HdfsRepository, store: FeatureStore, name: String): Hdfs[Unit] =
    InternalFeatureStoreStorer(repo, name).store(store)

  def storeToIvory(repository: S3Repository, store: FeatureStore, name: String): HdfsS3Action[Unit] =
    InternalFeatureStoreStorerS3(repository, name).store(store)
}
