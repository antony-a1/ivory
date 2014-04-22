package com.ambiata.ivory.repository.fatrepo

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import scala.math.{Ordering => SOrdering}
import org.apache.hadoop.fs.Path
import org.joda.time.{DateTimeZone, LocalDate}
import org.joda.time.format.DateTimeFormat
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.repository._
import com.ambiata.ivory.alien.hdfs._

/**
 * This workflow is designed to import dictionaries and features into an fat ivory repository,
 * one which contains all facts over all of time.
 *
 * Steps:
 * 1. Create empty repository if one doesn't exist
 * 2. Import dictionary:
 *    - Import the new dictionary, overwriting any existing files which clash.
 *    - Use todays date (yyyy-MM-dd) as the identifier for the new dictionaries
 * 3. Create an empty fact set to import the data feeds into
 * 4. Import the feeds into the fact set
 * 5. Create a new feature store:
 *    - Find the latest feature store
 *    - Create a new feature store containing the newly created fact set, and all the fact sets from the latest feature store
 *    - Use the previous feature store + 1 as the name of the new feature store
 */
object ImportWorkflow {

  type FactsetName = String
  type DictionaryName = String
  type DictionaryPath = Path
  type ErrorPath = Path
  type TmpPath = Path
  type Tombstone = List[String]
  type ImportDictFunc = (HdfsRepository, DictionaryName, Tombstone, TmpPath) => Hdfs[Unit]
  type ImportFactsFunc = (HdfsRepository, FactsetName, DictionaryName, TmpPath, ErrorPath, DateTimeZone) => ScoobiAction[Unit]

  private implicit val logger = LogFactory.getLog("ivory.repository.fatrepo.Import")

  def onHdfs(repoPath: Path, importDict: Option[ImportDictFunc], importFacts: ImportFactsFunc, tombstone: Tombstone, tmpPath: Path, errorPath: Path, timezone: DateTimeZone): ScoobiAction[String] = {
    val repo = Repository.fromHdfsPath(repoPath)
    val start = System.currentTimeMillis
    println("starting ==")
    for {
      _        <- ScoobiAction.fromHdfs(createRepo(repo))
      t1 = {
        val x = System.currentTimeMillis
        println(s"created repository in ${x - start}ms")
        x
      }
      dname    <- ScoobiAction.fromHdfs(importDictionary(repo, tombstone, new Path(tmpPath, "dictionaries"), importDict)
)
      t2 = {
        val x = System.currentTimeMillis
        println(s"imported dictionary in ${x - t1}ms")
        x
      }
      factset  <- ScoobiAction.fromHdfs(createFactSet(repo))
      t3 = {
        val x = System.currentTimeMillis
        println(s"created fact set in ${x - t2}ms")
        x
      }
      _        <- importFacts(repo, factset, dname, new Path(tmpPath, "facts"), new Path(errorPath, "facts"), timezone)
      t4 = {
        val x = System.currentTimeMillis
        println(s"imported fact set in ${x - t3}ms")
        x
      }
      sname    <- ScoobiAction.fromHdfs(createStore(repo, factset))
      t5 = {
        val x = System.currentTimeMillis
        println(s"created store in ${x - t4}ms")
        x
      }
    } yield sname
  }

  def createRepo(repo: HdfsRepository): Hdfs[Unit] = for {
    _  <- Hdfs.value(logger.debug(s"Going to create repository '${repo.path}'"))
    e  <- Hdfs.exists(repo.path)
    _  <- if(!e) {
      logger.debug(s"Hdfs path '${repo.path}' doesn't exist, creating")
      val res = CreateRepository.onHdfs(repo.path)
      logger.info(s"Repository '${repo.path}' created")
      res
    } else {
      logger.info(s"Repository already exists at '${repo.path}', not creating a new one")
      Hdfs.ok(())
    }
  } yield ()

  def importDictionary(repo: HdfsRepository, tombstone: List[String], tmpPath: Path, importer: Option[ImportDictFunc]): Hdfs[String] = importer match {
    case None =>
      Hdfs.globPaths(repo.dictionariesPath, "*").map(dicts =>
        dicts
          .map(_.getName)
          .filter(_.matches("""\d{4}-\d{2}-\d{2}"""))
          .map(DateTimeFormat.forPattern("yyyy-MM-dd").parseLocalDate)
          .sortBy(d => (d.getYear, d.getMonthOfYear, d.getDayOfMonth)).last.toString("yyyy-MM-dd")
      )
    case Some(importDict) => {
      val name = (new LocalDate()).toString("yyyy-MM-dd")
      logger.info(s"Importing dictionary under the name '${name}'")
      for {
        e <- Hdfs.exists(repo.dictionaryPath(name))
        _ <- if(!e) copyLatestDictionary(repo, name) else Hdfs.ok(())
        _ <- importDict(repo, name, tombstone, tmpPath)
        _  = logger.info(s"Successfully imported dictionary '${name}'")
      } yield name
    }
  }

  def copyLatestDictionary(repo: HdfsRepository, name: String): Hdfs[Unit] = for {
    _         <- Hdfs.value(logger.debug(s"Going to copy the latest dictionary to '${name}'"))
    dictPaths <- Hdfs.globPaths(repo.dictionariesPath)
    latest     = dictPaths.sortBy(_.getName)(SOrdering[String].reverse).headOption
    _         <- latest.traverse(l => {
                   val dest = repo.dictionaryPath(name)
                   logger.debug(s"Copying dictionary '${l}' to '${dest}'")
                   Hdfs.cp(l, dest, false)
                 })
  } yield ()

  def createFactSet(repo: HdfsRepository): Hdfs[String] = for {
    factsetPaths <- Hdfs.globPaths(repo.factsetsPath)
    name          = nextName(factsetPaths.map(_.getName))
    e            <- Hdfs.mkdir(repo.factsetPath(name))
    _            <- if(!e) Hdfs.fail("Hit race condition when trying to create fact set") else Hdfs.ok(())
  } yield name

  def createStore(repo: HdfsRepository, factset: String): Hdfs[String] = for {
    storeNames <- Hdfs.globPaths(repo.storesPath).map(_.map(_.getName))
    latest      = latestName(storeNames)
    name        = nextName(storeNames)
    _           = logger.debug(s"Going to create feature store '${name}'" + latest.map(l => s" based off feature store '${l}'").getOrElse(""))
    _          <- CreateFeatureStore.onHdfs(repo.path, name, List(factset), latest)
  } yield name

  def latestName(names: List[String]): Option[String] =
    latestNameWith(names, identity)

  def latestNameWith(names: List[String], incr: Int => Int): Option[String] = {
    val offsets = names.map(_.parseInt).collect({ case Success(i) => i })
    if(offsets.isEmpty) None else Some(zeroPad(incr(offsets.max)))
  }

  def firstName: String =
    zeroPad(0)

  def nextName(names: List[String]): String =
    latestNameWith(names, _ + 1).getOrElse(firstName)

  def zeroPad(i: Int): String =
    "%05d".format(i)
}
