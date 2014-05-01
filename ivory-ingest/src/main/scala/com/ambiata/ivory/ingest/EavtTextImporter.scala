package com.ambiata.ivory
package ingest

import org.apache.hadoop.fs.Path
import com.nicta.scoobi.Scoobi._
import core._
import storage.IvoryStorage
import storage.IvoryStorage._
import scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect.IO
import alien.hdfs._
import metadata.Versions
import storage.EavtTextStorageV1._
import ScoobiS3EMRAction._
import ScoobiAction._
import WireFormats._
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

  def onS3(repository: S3Repository, dictionary: Dictionary, factset: String, namespace: String, path: FilePath, timezone: DateTimeZone, codec: Option[CompressionCodec], preprocess: String => String = identity): ScoobiS3EMRAction[Unit] = for {
    _  <- ScoobiS3EMRAction.reader((sc: ScoobiConfiguration) =>
              basicScoobiJob(repository.hdfsRepository, dictionary, factset, namespace,
                new Path(path.path), new Path(repository.tmpDirectory+"/errors/"), timezone, codec, preprocess)(sc))
    _  <- copyFilesToS3(repository, factset, namespace)
  } yield ()

  def onHdfs(repository: HdfsRepository, dictionary: Dictionary, factset: String, namespace: String,
             path: Path, errorPath: Path, timezone: DateTimeZone, codec: Option[CompressionCodec],
              preprocess: String => String = identity): ScoobiAction[Unit] = for {
    sc <- ScoobiAction.scoobiConfiguration
    _  <- ScoobiAction.safe(basicScoobiJob(repository, dictionary, factset, namespace, path, errorPath, timezone, codec, preprocess)(sc))
    _  <- ScoobiAction.fromHdfs(writeFactsetVersion(repository, List(factset)))
  } yield ()

  // FIX horrible duplication, this all needs to be reformulated into a composable pipeline
  def onHdfsBulk(repository: HdfsRepository, dictionary: Dictionary, factset: String, namespace: List[String],
             path: Path, errorPath: Path, timezone: DateTimeZone, codec: Option[CompressionCodec], partitions: Map[String, Int], preprocess: String => String = identity): ScoobiAction[Unit] = for {
    sc <- ScoobiAction.scoobiConfiguration
    _  <- ScoobiAction.safe(compoundScoobiJob(repository, dictionary, factset, namespace, path, errorPath, timezone, codec, partitions, preprocess)(sc))
    _  <- ScoobiAction.fromHdfs(writeFactsetVersion(repository, List(factset)))
  } yield ()

  def onHdfsDirect(conf: Configuration, repository: HdfsRepository, dictionary: Dictionary, factset: String, namespace: String,
             path: Path, errorPath: Path, timezone: DateTimeZone, codec: Option[CompressionCodec],
             preprocess: String => String): ResultT[IO, Unit] = for {
    _  <- HdfsDirectEavtTextImporter.direct(conf, repository, dictionary, factset, namespace, path, errorPath, timezone, codec, preprocess)
    _  <- writeFactsetVersion(repository, List(factset)).run(conf)
  } yield ()

  type KeyedBy[A] = Fact => A

  val keyedByEntityFeature: KeyedBy[(String, String)] =
    f => (f.entity, f.featureId.name)

  val keyedByPartition: KeyedBy[String] =
    f => s"${f.namespace}/${f.date.string("/")}"

  // FIX lots of duplication with RawFeatureThriftImporter and below
  def compoundScoobiJob(
    repository: HdfsRepository,
    dictionary: Dictionary,
    factset: String,
    namespaces: List[String],
    path: Path,
    errorPath: Path,
    timezone: DateTimeZone,
    codec: Option[CompressionCodec],
    partitions: Map[String, Int],
    preprocess: String => String
  )(implicit
    sc: ScoobiConfiguration
  ) {
    sc.setMinReducers(partitions.size)
    val parsedFacts = namespaces.map(namespace => {
      fromEavtTextFile(path.toString + "/" + namespace + "/*", dictionary, namespace, timezone, preprocess)
    }).reduceLeft(_ ++ _)

    scoobiJobOnFacts(
      parsedFacts,
      repository,
      factset,
      path,
      errorPath,
      codec,
      keyedByPartition,
      Groupings.partitionGrouping(partitions)
    )
  }

  // FIX lots of duplication with RawFeatureThriftImporter
  def basicScoobiJob(
    repository: HdfsRepository,
    dictionary: Dictionary,
    factset: String,
    namespace: String,
    path: Path,
    errorPath: Path,
    timezone: DateTimeZone,
    codec: Option[CompressionCodec],
    preprocess: String => String
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
      codec,
      keyedByEntityFeature,
      Groupings.sortGrouping
    )
  }

  // FIX lots of duplication with RawFeatureThriftImporter
  def scoobiJobOnFacts[A](
    dlist: DList[String \/ Fact],
    repository: HdfsRepository,
    factset: String,
    path: Path,
    errorPath: Path,
    codec: Option[CompressionCodec],
    keyedBy: KeyedBy[A],
    grouping: Grouping[A]
  )(implicit
    sc: ScoobiConfiguration,
    A: WireFormat[A]
  ) {
    implicit val FactWireFormat = WireFormats.FactWireFormat

    val errors: DList[String] = dlist.collect { case -\/(err) => err + " - path " + path }
    val facts: DList[Fact]    = dlist.collect { case \/-(f) => f }

    val packed =
      facts
        .by(keyedBy)
        .groupByKeyWith(grouping)
        .mapFlatten(_._2)
        .toIvoryFactset(repository, factset)

    val compressed = codec match {
      case None => packed
      case Some(c) => packed.compressWith(c)
    }

    persist(compressed, errors.toTextFile(errorPath.toString, overwrite = true))
  }

  def copyFilesToS3(repository: S3Repository, factset: String, namespace: String): ScoobiS3EMRAction[Unit] = for {
    sc <- ScoobiS3EMRAction.scoobiConfiguration
    _  <- Option(sc.get("EMR_CLUSTER_ID")).map((clusterId: String) => copyFilesWithDistCp(clusterId, repository, factset, namespace)).
           getOrElse(copyFilesOneByOne(repository, factset, namespace))
  } yield ()

  def copyFilesOneByOne(repository: S3Repository, factset: String, namespace: String): ScoobiS3EMRAction[Unit] =
    ScoobiS3EMRAction.fromHdfsS3(HdfsS3.putPathsByDate(repository.bucket, repository.factsetKey(factset)+"/"+namespace, new Path(repository.hdfsRepository.factsetPath(factset), namespace)))

  def copyFilesWithDistCp(clusterId: String, repository: S3Repository, factset: String, namespace: String): ScoobiS3EMRAction[Unit] = {
    val src  = s"hdfs:///${repository.hdfsRepository.factsetPath(factset)+"/"+namespace}"
    val dest = s"s3://${repository.bucket}/${repository.factsetKey(factset)+"/"+namespace}"
    ScoobiS3EMRAction.fromEMRAction(DistCopy.run(clusterId, List(s"--src=$src", s"--dest=$dest", "--srcPattern=.*/.*/.*"))).void
  }

}
