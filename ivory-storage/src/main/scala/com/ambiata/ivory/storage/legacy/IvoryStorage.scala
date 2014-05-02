package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import com.ambiata.saws.core._

import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.ScoobiS3Action._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.alien.hdfs.HdfsS3Action._
import com.ambiata.saws.s3.S3
import ScoobiS3Action._
import com.ambiata.ivory.alien.hdfs._
import WireFormats._
import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs.HdfsS3Action

trait IvoryLoader[A] {
  def load: A
}

trait IvoryStorer[A, B] {
  def store(a: A): B
}

trait IvoryScoobiLoader[A] {
  def loadScoobi(implicit sc: ScoobiConfiguration): DList[String \/ A]
}

trait IvoryScoobiStorer[A, +B] {
  def storeScoobi(dlist: DList[A])(implicit sc: ScoobiConfiguration): B
  def storeMeta: ScoobiAction[Unit] =
    ScoobiAction.ok(())
}

/**
 * Fact loaders/storers
 */
case class InternalFactsetFactLoader(repo: HdfsRepository, factset: String) {
  def load: ScoobiAction[DList[String \/ Fact]] = for {
    sc <- ScoobiAction.scoobiConfiguration
    v  <- ScoobiAction.fromHdfs(Versions.readFactsetVersionFromHdfs(repo, factset))
    l   = IvoryStorage.factsetLoader(v, repo.factsetPath(factset))
  } yield l.loadScoobi(sc)
}

case class InternalFactsetFactS3Loader(repo: S3Repository, factset: String) {
  def load: ScoobiS3Action[DList[String \/ Fact]] = for {
    sc <- ScoobiS3Action.scoobiConfiguration
    v  <- ScoobiS3Action.fromS3Action(Versions.readFactsetVersionFromS3(repo, factset))
  } yield IvoryStorage.factsetLoader(v, new Path("s3://"+repo.bucket+"/"+repo.factsetKey(factset))).loadScoobi(sc)
}

case class InternalFeatureStoreFactLoader(repo: HdfsRepository, store: FeatureStore) {
  def load: ScoobiAction[DList[String \/ (Priority, FactSetName, Fact)]] = for {
    sc       <- ScoobiAction.scoobiConfiguration
    versions <- store.factSets.traverseU(factset => ScoobiAction.fromHdfs(Versions.readFactsetVersionFromHdfs(repo, factset.name).map((factset, _))))
    combined: List[(FactsetVersion, List[FactSet])] = versions.groupBy(_._2).toList.map({ case (k, vs) => (k, vs.map(_._1)) })
  } yield combined.map({ case (v, fss) => IvoryStorage.multiFactsetLoader(v, repo.factsetsPath, fss).loadScoobi(sc) }).reduce(_++_)
}

case class InternalFactsetFactStorer(repo: HdfsRepository, factset: String) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
  def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration) =
    IvoryStorage.factsetStorer(repo.factsetPath(factset).toString).storeScoobi(dlist)
}

/**
 * Dictionary loaders/storers
 */
case class InternalDictionaryLoader(repo: HdfsRepository, name: String) extends IvoryLoader[Hdfs[Dictionary]] {
  import DictionaryTextStorage._
  def load: Hdfs[Dictionary] =
    DictionaryTextLoader(repo.dictionaryPath(name)).load
}

case class InternalDictionaryStorer(repo: HdfsRepository, name: String) extends IvoryStorer[Dictionary, Hdfs[Unit]] {
  import DictionaryTextStorage._
  def store(dict: Dictionary): Hdfs[Unit] =
    DictionaryTextStorer(repo.dictionaryPath(name)).store(dict)
}

case class InternalDictionariesStorer(repo: HdfsRepository, name: String) extends IvoryStorer[List[Dictionary], Hdfs[Unit]] {
  import DictionaryTextStorage._
  def store(dicts: List[Dictionary]): Hdfs[Unit] =
    dicts.traverse(d => DictionaryTextStorer(new Path(repo.dictionaryPath(name), d.name)).store(d)).map(_ => ())
}

case class DictionariesS3Storer(repository: S3Repository) {
  import DictionaryTextStorage._

  def store(dictionary: Dictionary, name: String): HdfsS3Action[Unit] = {
    val tmpPath = new Path(repository.tmpDirectory, repository.dictionaryKey(name))
    for {
      _ <- HdfsS3Action.fromHdfs(DictionaryTextStorer(tmpPath).store(dictionary))
      a <- HdfsS3.putPaths(repository.bucket, repository.dictionaryKey(name), new Path(tmpPath, "*"))
      _ <- HdfsS3Action.fromHdfs(Hdfs.filesystem.map(fs => fs.delete(tmpPath, true)))
    } yield a
  }

  def store(dictionaries: List[Dictionary], name: String): HdfsS3Action[Unit] =
    for {
      _ <- HdfsS3Action.fromHdfs(Hdfs.filesystem.map(fs => fs.mkdirs(new Path(repository.tmpDirectory))))
      _ <- HdfsS3Action.fromHdfs(dictionaries.traverse(d => DictionaryTextStorer(new Path(repository.tmpDirectory+"/"+repository.dictionaryKey(name), d.name)).store(d)))
      a <- HdfsS3.putPaths(repository.bucket, repository.dictionaryKey(name), new Path(repository.tmpDirectory+"/"+repository.dictionariesKey), "*")
      _ <- HdfsS3Action.fromHdfs(Hdfs.filesystem.map(fs => fs.delete(new Path(repository.tmpDirectory), true)))
    } yield a
}

case class DictionariesS3Loader(repository: S3Repository) {
  import DictionaryTextStorage._

  def load(dictionaryName: String): HdfsS3Action[Dictionary] = for {
    _ <- HdfsS3Action.fromAction(S3.downloadFile(repository.bucket, repository.dictionaryKey(dictionaryName), repository.tmpDirectory+"/"+dictionaryName))
    d <- HdfsS3Action.fromHdfs(DictionaryTextLoader(new Path(repository.tmpDirectory+"/"+dictionaryName)).load)
  } yield d
}


/**
 * Feature store loaders/storers
 */
case class InternalFeatureStoreLoader(repo: HdfsRepository, name: String) extends IvoryLoader[Hdfs[FeatureStore]] {
  import FeatureStoreTextStorage._
  def load: Hdfs[FeatureStore] =
    FeatureStoreTextLoader(repo.storePath(name)).load
}

case class InternalFeatureStoreLoaderS3(repository: S3Repository, name: String) {
  import FeatureStoreTextStorage._
  def load: HdfsS3Action[FeatureStore] = for {
    file  <- HdfsS3Action.fromAction(S3.downloadFile(repository.bucket, repository.storeKey(name), to = repository.tmpDirectory+"/"+name))
    store <- HdfsS3Action.fromHdfs(FeatureStoreTextLoader(new Path(file.getPath)).load)
  } yield store
}

case class InternalFeatureStoreStorer(repo: HdfsRepository, name: String) extends IvoryStorer[FeatureStore, Hdfs[Unit]] {
  import FeatureStoreTextStorage._
  def store(store: FeatureStore): Hdfs[Unit] =
    FeatureStoreTextStorer(repo.storePath(name)).store(store)
}

case class InternalFeatureStoreStorerS3(repository: S3Repository, name: String) {
  import FeatureStoreTextStorage._
  def store(store: FeatureStore): HdfsS3Action[Unit] = {
    val tmpPath = new Path(repository.tmpDirectory, repository.storeKey(name))
    for {
      _ <- HdfsS3Action.fromHdfs(FeatureStoreTextStorer(tmpPath).store(store))
      _ <- HdfsS3.putPaths(repository.bucket, repository.storeKey(name), tmpPath, glob = "*")
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
  def factsetLoader(version: FactsetVersion, path: Path): IvoryScoobiLoader[Fact] = version match {
    case FactsetVersionOne => PartitionFactThriftStorageV1.PartitionedFactThriftLoader(path.toString)
    case FactsetVersionTwo => PartitionFactThriftStorageV2.PartitionedFactThriftLoader(path.toString)
  }

  def multiFactsetLoader(version: FactsetVersion, path: Path, factsets: List[FactSet]): IvoryScoobiLoader[(Int, String, Fact)] = version match {
    case FactsetVersionOne => PartitionFactThriftStorageV1.PartitionedMultiFactsetThriftLoader(path.toString, factsets)
    case FactsetVersionTwo => PartitionFactThriftStorageV2.PartitionedMultiFactsetThriftLoader(path.toString, factsets)
  }

  def writeFactsetVersion(repo: HdfsRepository, factsets: List[String]): Hdfs[Unit] =
    Versions.writeFactsetVersionToHdfs(repo, factsetVersion, factsets)

  implicit class IvoryFactStorage(dlist: DList[Fact]) {
    def toIvoryFactset(repo: HdfsRepository, factset: String)(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] =
      InternalFactsetFactStorer(repo, factset).storeScoobi(dlist)(sc)
  }

  def factsFromIvoryStore(repo: HdfsRepository, store: FeatureStore): ScoobiAction[DList[String \/ (Priority, FactSetName, Fact)]] =
    InternalFeatureStoreFactLoader(repo, store).load

  def factsFromIvoryFactset(repo: HdfsRepository, factset: String): ScoobiAction[DList[String \/ Fact]] =
    InternalFactsetFactLoader(repo, factset).load

  def factsFromIvoryFactset(repository: S3Repository, factset: String): ScoobiS3Action[DList[String \/ Fact]] =
    InternalFactsetFactS3Loader(repository, factset).load

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

  def storeFromIvory(repo: HdfsRepository, name: String): Hdfs[FeatureStore] =
    InternalFeatureStoreLoader(repo, name).load

  def storeFromIvory(repository: S3Repository, name: String): HdfsS3Action[FeatureStore] =
    InternalFeatureStoreLoaderS3(repository, name).load

  def storeToIvory(repo: HdfsRepository, store: FeatureStore, name: String): Hdfs[Unit] =
    InternalFeatureStoreStorer(repo, name).store(store)

  def storeToIvory(repository: S3Repository, store: FeatureStore, name: String): HdfsS3Action[Unit] =
    InternalFeatureStoreStorerS3(repository, name).store(store)
}
