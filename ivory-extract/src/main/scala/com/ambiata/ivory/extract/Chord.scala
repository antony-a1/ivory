package com.ambiata.ivory.extract

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import java.util.HashMap
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.io._
import com.ambiata.mundane.time.DateTimex
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.validate.Validate
import com.ambiata.ivory.alien.hdfs._

case class HdfsChord(repoPath: Path, store: String, dictName: String, entities: HashMap[String, Array[Int]], outputPath: Path, tmpPath: Path, errorPath: Path, incremental: Option[Path]) {
  import IvoryStorage._

  type PackedDate = Int
  // mappings of each entity to an array of target dates, represented as Ints and sorted from more recent to least
  type Mappings   = HashMap[String, Array[PackedDate]]

  // lexical order for a pair (Fact, Priority) so that
  // p1 < p2 <==> f1.datetime > f2.datetime || f1.datetime == f2.datetime && priority1 < priority2
  implicit val ord: Order[(Fact, Priority)] = Order.orderBy { case (f, p) => (-f.datetime.long, p) }

  val ChordName: String = "ivory-incremental-chord"

  def run: ScoobiAction[Unit] = for {
    c  <- ScoobiAction.scoobiConfiguration
    r  <- ScoobiAction.value(Repository.fromHdfsPath(repoPath.toString.toFilePath, c))
    d  <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dictName))
    s  <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    (earliest, latest) = DateMap.bounds(entities)
    chordPath = new Path(tmpPath, java.util.UUID.randomUUID().toString)
    _  <- ScoobiAction.fromHdfs(Chord.serialiseChords(chordPath, entities))
    in <- incremental.traverseU(path => for {
      sm <- ScoobiAction.fromHdfs(SnapshotMeta.fromHdfs(new Path(path, ".snapmeta")))
      _   = println(s"Snapshot store was '${sm.store}'")
      _   = println(s"Snapshot date was '${sm.date.string("-")}'")
      s  <- ScoobiAction.fromHdfs(storeFromIvory(r, sm.store))
    } yield (path, s, sm))
    _  <- scoobiJob(r, d, s, chordPath, latest, validateIncr(earliest, in))
    _  <- ScoobiAction.fromHdfs(DictionaryTextStorage.DictionaryTextStorer(new Path(outputPath, ".dictionary")).store(d))
  } yield ()

  def validateIncr(earliest: Date, in: Option[(Path, FeatureStore, SnapshotMeta)]): Option[(Path, FeatureStore, SnapshotMeta)] =
    in.flatMap({ case i @ (path, s, sm) =>
      if(earliest isBefore sm.date) {
        println(s"Earliest date '${earliest}' in chord file is before snapshot date '${sm.date}' so going to skip incremental and pass over all data.")
        None
      } else {
        Some(i)
      }
    })

  /**
   * Persist facts which are the latest corresponding to a set of dates given for each entity
   */
  def scoobiJob(repo: HdfsRepository, dict: Dictionary, store: FeatureStore, chordPath: Path, latestDate: Date, incremental: Option[(Path, FeatureStore, SnapshotMeta)]): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      lazy val factsetMap: Map[Priority, Factset] = store.factsets.map(fs => (fs.priority, fs.set)).toMap

      HdfsSnapshot.readFacts(repo, store, latestDate, incremental).map { input =>

        // DANGER - needs to be executed on mapper
        lazy val map: Mappings = Chord.deserialiseChords(chordPath).run(sc).run.unsafePerformIO() match {
          case Ok(m)    => m
          case Error(e) => sys.error("Can not deserialise chord map - " + Result.asString(e))
        }

        // filter out the facts which are not in the entityMap or
        // which date are greater than the required dates for this entity
        val facts: DList[(Priority, Fact)] = input.map({
          case -\/(e) => sys.error("A critical error has occured, where we could not determine priority and namespace from partitioning: " + e)
          case \/-(v) => v
        }).collect({
          case (p, _, f) if(DateMap.keep(map, f.entity, f.date.year, f.date.month, f.date.day)) => (p, f)
        })

        /**
         * 1. group by entity and feature id
         * 2. for a given entity and feature id, get the latest facts, with the lowest priority
         */
        val latest: DList[(Priority, Fact)] =
          facts
            .groupBy { case (p, f) => (f.entity, f.featureId.toString) }
            .mapFlatten { case ((entityId, featureId), fs) =>
              // the required dates
              val dates = map.get(entityId)

              // we traverse all facts and for each required date
              // we keep the "best" fact which date is just before that date
              fs.foldLeft(dates.map((_, Priority.Min, None)): Array[(Int, Priority, Option[Fact])]) { case (ds, (priority, fact)) =>
                val factDate = fact.date.int
                ds.map {
                  case previous @ (date, p, None)    =>
                    // we found a first suitable fact for that date
                    if (factDate <= date) (date, priority, Some(fact))
                    else                  previous

                  case previous @ (date, p, Some(f)) =>
                    // we found a fact with a better time, or better priority if there is a tie
                    if (factDate <= date && (fact, priority) < ((f, p))) (date, priority, Some(fact))
                    else                                               previous
                }
              }.collect { case (d, p, Some(f)) => (p, f.withEntity(f.entity + ":" + Date.unsafeFromInt(d).hyphenated)) }.toIterable
            }.collect { case (p, f) if !f.isTombstone => (p, f) }

        val validated: DList[Fact] = latest.map({ case (p, f) =>
          Validate.validateFact(f, dict).disjunction.leftMap(e => e + " - Factset " + factsetMap.get(p).getOrElse("Unknown, priority " + p))
        }).map({
          case -\/(e) => sys.error("A critical error has occurred, a value in ivory no longer matches the dictionary: " + e)
          case \/-(v) => v
        })

        persist(validated.valueToSequenceFile(outputPath.toString, overwrite = true).compressWith(new SnappyCodec))

        ()
      }
    }).flatten
}

object Chord {

  def onHdfs(repoPath: Path, entities: Path, outputPath: Path, tmpPath: Path, errorPath: Path): ScoobiAction[Unit] = for {
    es                  <- ScoobiAction.fromHdfs(Chord.readChords(entities))
    (earliest, latest)   = DateMap.bounds(es)
    _                    = println(s"Earliest date in chord file is '${earliest}'")
    _                    = println(s"Latest date in chord file is '${latest}'")
    snap                <- HdfsSnapshot.takeSnapshot(repoPath, errorPath, earliest, true)
    (store, dname, path) = snap
    _                   <- HdfsChord(repoPath, store, dname, es, outputPath, tmpPath, errorPath, Some(path)).run
  } yield ()

  def serialiseChords(path: Path, map: HashMap[String, Array[Int]]): Hdfs[Unit] = {
    import java.io.ObjectOutputStream
    Hdfs.writeWith(path, os => ResultT.safe({
      val bOut = new ObjectOutputStream(os)
      bOut.writeObject(map)
      bOut.close()
    }))
  }

  def deserialiseChords(path: Path): Hdfs[HashMap[String, Array[Int]]] = {
    import java.io.ObjectInputStream
    Hdfs.readWith(path, is => ResultT.safe((new ObjectInputStream(is)).readObject.asInstanceOf[HashMap[String, Array[Int]]]))
  }

  def readChords(path: Path): Hdfs[HashMap[String, Array[Int]]] = for {
    chords <- Hdfs.readWith(path, is => Streams.read(is))
  } yield DateMap.chords(chords)
}
