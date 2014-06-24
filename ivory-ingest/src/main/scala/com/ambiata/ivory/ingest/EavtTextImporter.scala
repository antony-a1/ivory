package com.ambiata.ivory
package ingest

import org.apache.hadoop.fs.Path
import com.nicta.scoobi.Scoobi._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.legacy.IvoryStorage
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.legacy.EavtTextStorageV1._
import com.ambiata.ivory.storage.repository._
import scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect.IO
import alien.hdfs._
import ScoobiS3EMRAction._
import ScoobiAction._
import WireFormats._, FactFormats._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.FilePath
import com.ambiata.saws.emr._
import org.joda.time.DateTimeZone
import org.apache.hadoop.io.compress._
import org.apache.hadoop.conf.Configuration

// FIX move to com.ambiata.ivory.ingest.internal
/**
 * Import a text file, formatted as an EAVT file, into ivory
 * either on S3 or Hdfs
 */
object EavtTextImporter {

  def onS3(repository: S3Repository, dictionary: Dictionary, factset: Factset, namespace: String, path: FilePath, timezone: DateTimeZone, codec: Option[CompressionCodec], preprocess: String => String = identity): ScoobiS3EMRAction[Unit] = for {
    _  <- ScoobiS3EMRAction.reader((sc: ScoobiConfiguration) =>
              basicScoobiJob(repository.hdfs, dictionary, factset, namespace,
                new Path(path.path), (repository.tmp </> "errors").toHdfs, timezone, preprocess, codec)(sc))
    _  <- copyFilesToS3(repository, factset, namespace)
  } yield ()

  def onHdfs(repository: HdfsRepository, dictionary: Dictionary, factset: Factset, namespace: String,
             path: Path, errorPath: Path, timezone: DateTimeZone,
             codec: Option[CompressionCodec],
              preprocess: String => String = identity): ScoobiAction[Unit] = for {
    sc <- ScoobiAction.scoobiConfiguration
    _  <- ScoobiAction.safe(basicScoobiJob(repository, dictionary, factset, namespace, path, errorPath, timezone, preprocess, codec)(sc))
    _  <- ScoobiAction.fromHdfs(writeFactsetVersion(repository, List(factset)))
  } yield ()

  // FIX horrible duplication, this all needs to be reformulated into a composable pipeline
  def onHdfsBulk(repository: HdfsRepository, dictionary: Dictionary, factset: Factset, namespace: List[String],
             path: Path, errorPath: Path, timezone: DateTimeZone, partitions: List[(String, Long)], optimal: Long, codec: Option[CompressionCodec], preprocess: String => String = identity): ScoobiAction[Unit] = for {
    sc <- ScoobiAction.scoobiConfiguration
    _  <- ScoobiAction.safe(compoundScoobiJob(repository, dictionary, factset, namespace, path, errorPath, timezone, partitions, optimal, preprocess, codec)(sc))
    _  <- ScoobiAction.fromHdfs(writeFactsetVersion(repository, List(factset)))
  } yield ()

  def onHdfsDirect(conf: Configuration, repository: HdfsRepository, dictionary: Dictionary, factset: Factset, namespace: String,
             path: Path, errorPath: Path, timezone: DateTimeZone,
             preprocess: String => String): ResultT[IO, Unit] = for {
    _  <- HdfsDirectEavtTextImporter.direct(conf, repository, dictionary, factset, namespace, path, errorPath, timezone, preprocess)
    _  <- writeFactsetVersion(repository, List(factset)).run(conf)
  } yield ()

  type KeyedBy[A] = Fact => A

  val keyedByEntityFeature: KeyedBy[(String, String)] =
    f => (f.entity, f.featureId.name)

  val keyedByPartition: KeyedBy[String] =
    f => s"${f.namespace}/${f.date.string("/")}"

  def compoundScoobiJob(
    repository: HdfsRepository,
    dictionary: Dictionary,
    factset: Factset,
    namespaces: List[String],
    root: Path,
    errorPath: Path,
    timezone: DateTimeZone,
    partitions: List[(String, Long)],
    optimal: Long,
    preprocess: String => String,
    codec: Option[CompressionCodec]
  )(implicit
    sc: ScoobiConfiguration
  ) {

    def index(dict: Dictionary): (NamespaceLookup, FeatureIdLookup) = {
      val namespaces = new NamespaceLookup
      val features = new FeatureIdLookup
      dict.meta.toList.zipWithIndex.foreach({ case ((fid, _), idx) =>
        namespaces.putToNamespaces(idx, fid.namespace)
                                             features.putToIds(fid.toString, idx)
                                           })
      (namespaces, features)
    }
    val (reducers, allocations) = Skew.calculate(dictionary, partitions, optimal)
    val (namespaces, features) = index(dictionary)
    val indexed = new ReducerLookup
    allocations.foreach({ case (n, f, r) =>
      indexed.putToReducers(features.ids.get(FeatureId(n, f).toString), r) })
    IngestJob.run(sc, reducers, indexed, namespaces, features, dictionary, timezone, timezone, root, partitions.map(_._1).map(namespace => root.toString + "/" + namespace + "/*"), repository.factset(factset).toHdfs, errorPath, codec)
  }


  // FIX lots of duplication with RawFeatureThriftImporter
  def basicScoobiJob(
    repository: HdfsRepository,
    dictionary: Dictionary,
    factset: Factset,
    namespace: String,
    path: Path,
    errorPath: Path,
    timezone: DateTimeZone,
    preprocess: String => String,
    codec: Option[CompressionCodec]
  )(implicit
    sc: ScoobiConfiguration
  ) {
    val parsedFacts = fromEavtTextFile(
      path.toString, dictionary, namespace, timezone, preprocess)

    scoobiJobOnFacts(
      parsedFacts,
      repository,
      factset,
      path,
      errorPath,
      keyedByEntityFeature,
      Groupings.sortGrouping,
      codec
    )
  }

  // FIX lots of duplication with RawFeatureThriftImporter
  def scoobiJobOnFacts[A](
    dlist: DList[ParseError \/ Fact],
    repository: HdfsRepository,
    factset: Factset,
    path: Path,
    errorPath: Path,
    keyedBy: KeyedBy[A],
    grouping: Grouping[A],
    codec: Option[CompressionCodec]
  )(implicit
    sc: ScoobiConfiguration,
    A: WireFormat[A]
  ) {
    val errors: DList[ParseError] = dlist.collect { case -\/(err) => err.appendToMessage(" - path " + path) }
    val facts: DList[Fact]    = dlist.collect { case \/-(f) => f }

    val packed =
      facts
        .by(keyedBy)
        .groupByKeyWith(grouping)
        .mapFlatten(_._2)
        .toIvoryFactset(repository, factset, codec)

    val persistErrors =
      errors.valueToSequenceFile(errorPath.toString, overwrite = true)

    persist(codec.map(packed.compressWith(_)).getOrElse(packed),
            codec.map(persistErrors.compressWith(_)).getOrElse(persistErrors))
  }

  def copyFilesToS3(repository: S3Repository, factset: Factset, namespace: String): ScoobiS3EMRAction[Unit] = for {
    sc <- ScoobiS3EMRAction.scoobiConfiguration
    _  <- Option(sc.get("EMR_CLUSTER_ID")).map((clusterId: String) => copyFilesWithDistCp(clusterId, repository, factset, namespace)).
           getOrElse(copyFilesOneByOne(repository, factset, namespace))
  } yield ()

  def copyFilesOneByOne(repository: S3Repository, factset: Factset, namespace: String): ScoobiS3EMRAction[Unit] = {
    ScoobiS3EMRAction.fromHdfsS3(HdfsS3.putPathsByDate(repository.bucket, repository.namespace(factset, namespace).path, (repository.hdfs.factset(factset) </> namespace).toHdfs))
  }

  def copyFilesWithDistCp(clusterId: String, repository: S3Repository, factset: Factset, namespace: String): ScoobiS3EMRAction[Unit] = {
    val src  = s"hdfs:///${repository.hdfs.factset(factset)}/${namespace}"
    val dest = s"s3://${repository.bucket}/${repository.namespace(factset, namespace).path}"
    ScoobiS3EMRAction.fromEMRAction(DistCopy.run(clusterId, List(s"--src=$src", s"--dest=$dest", "--srcPattern=.*/.*/.*"))).void
  }

}
