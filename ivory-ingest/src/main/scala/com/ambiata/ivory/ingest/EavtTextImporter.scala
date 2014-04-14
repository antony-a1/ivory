package com.ambiata.ivory
package ingest

import org.apache.hadoop.fs.Path
import com.nicta.scoobi.Scoobi._
import core._
import storage.IvoryStorage
import storage.IvoryStorage._
import scoobi._
import scalaz.{DList => _, _}, Scalaz._
import alien.hdfs._
import metadata.Versions
import storage.EavtTextStorage._
import ScoobiS3EMRAction._
import ScoobiAction._
import WireFormats._
import com.ambiata.mundane.io.FilePath
import com.ambiata.saws.emr._
import org.joda.time.DateTimeZone

/**
 * Import a text file, formatted as an EAVT file, into ivory
 * either on S3 or Hdfs
 */
object EavtTextImporter {

  def onS3(repository: S3Repository, dictionary: Dictionary, factset: String, namespace: String, path: FilePath, timezone: DateTimeZone, preprocess: String => String = identity): ScoobiS3EMRAction[Unit] = for {
    _  <- ScoobiS3EMRAction.reader((sc: ScoobiConfiguration) =>
              scoobiJob(repository.hdfsRepository, dictionary, factset, namespace,
                new Path(path.path), new Path(repository.tmpDirectory+"/errors/"), timezone,
                preprocess)(sc))
    _  <- copyFilesToS3(repository, factset, namespace)
  } yield ()

  def onHdfs(repository: HdfsRepository, dictionary: Dictionary, factset: String, namespace: String,
             path: Path, errorPath: Path, timezone: DateTimeZone,
             preprocess: String => String = identity): ScoobiAction[Unit] = for {
    sc <- ScoobiAction.scoobiConfiguration
    _  <- ScoobiAction.safe(scoobiJob(repository, dictionary, factset, namespace, path, errorPath, timezone, preprocess)(sc))
    _  <- ScoobiAction.fromHdfs(writeFactsetVersion(repository, List(factset)))
  } yield ()

  def scoobiJob(repository: HdfsRepository, dictionary: Dictionary, factset: String, namespace: String,
                path: Path, errorPath: Path, timezone: DateTimeZone,
                preprocess: String => String = identity)(implicit sc: ScoobiConfiguration) {
    val parsedFacts = fromEavtTextFile(path.toString, dictionary, namespace, timezone, preprocess)

    val errors: DList[String] = parsedFacts.collect { case -\/(err) => err + " - path " + path }
    val facts: DList[Fact]    = parsedFacts.collect { case \/-(f) => f }

    persist(errors.toTextFile(errorPath.toString, overwrite = true), facts.toIvoryFactset(repository, factset))
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
