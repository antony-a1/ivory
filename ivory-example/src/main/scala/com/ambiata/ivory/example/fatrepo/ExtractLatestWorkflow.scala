package com.ambiata.ivory.example.fatrepo

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._
import com.ambiata.ivory.extract._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.alien.hdfs._

/**
 * This workflow is designed to extract the latest features from a feature store in a fat repo, one which contains all facts over all of time
 *
 * Steps:
 * 1. Find the latest version of a feature store and dictionary.
 *    - This will get a listing of all feature stores and dictionaries, ordering them by name, and taking the last (latest)
 * 2. Extract the most recent version of every feature for every entity.
 *    - Run snapshot app with discovered store and dictionary
 *    - Store in sparse row thrift format
 */
object ExtractLatestWorkflow {

  private implicit val logger = LogFactory.getLog("ivory.example.fatrepo.ExtractLatestWorkflow")

  def onHdfs(repoPath: Path, outputPath: Path, errorPath: Path, date: LocalDate): ScoobiAction[(String, String)] =
    fatrepo.ExtractLatestWorkflow.onHdfs(repoPath, extractLatest(outputPath, errorPath), date)

  def extractLatest(outputPath: Path, errorPath: Path)(repo: HdfsRepository, store: String, dictName: String, date: LocalDate): ScoobiAction[Unit] = for {
    _ <- ScoobiAction.value(logger.info(s"Extracting latest features from '${date.toString("yyyy-MM-dd")}' using the store '${store}' and dictionary '${dictName}', from the '${repo.path}' repository. Output '${outputPath}'. Errors '${errorPath}'"))
    _ <- HdfsSnapshot(repo.path, store, dictName, None, date, outputPath, errorPath, None).run
    _ <- storeInFormat(new Path(outputPath, "thrift"), new Path(outputPath, "eavt"), new Path(errorPath, "snapshot"))
    _  = logger.info(s"Successfully extracted latest features to '${outputPath}'")
  } yield ()

  def storeInFormat(inputPath: Path, outputPath: Path, errorPath: Path): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      val in: DList[String \/ Fact] = PartitionFactThriftStorageV2.PartitionedFactThriftLoader(inputPath.toString).loadScoobi

      val errors: DList[String] = in.collect({
        case -\/(e) => e
      })

      val good: DList[Fact] = in.collect({
        case \/-(f) => f
      })

      persist(EavtTextStorageV1.EavtTextStorer(outputPath.toString).storeScoobi(good), errors.toTextFile(errorPath.toString))
    })
}
