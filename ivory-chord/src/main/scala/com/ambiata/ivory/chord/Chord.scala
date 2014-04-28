package com.ambiata.ivory.chord

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import org.joda.time.{LocalDate, LocalDateTime, LocalTime}
import org.joda.time.format.DateTimeFormat
import java.util.HashMap
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.mundane.time.DateTimex
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage._
import com.ambiata.ivory.validate.Validate
import com.ambiata.ivory.alien.hdfs._

case class HdfsChord(repoPath: Path, store: String, dictName: String, entities: Path, tmpPath: Path, errorPath: Path, storer: IvoryScoobiStorer[Fact, DList[_]]) {
  import IvoryStorage._

  type Priority   = Short
  type PackedDate = Int
  // mappings of each entity to an array of target dates, represented as Ints and sorted from more recent to least
  type Mappings   = HashMap[String, Array[PackedDate]]

  // lexical order for a pair (Fact, Priority) so that
  // p1 < p2 <==> f1.time > f2.time || f1.time == f2 && priority1 < priority2
  implicit val latestOrder: Order[LocalDateTime] = DateTimex.LocalDateTimeHasOrder.reverseOrder
  implicit val ord: Order[(Fact, Priority)] = Order.orderBy { case (f, p) => (f.time, p) }


  def withStorer(newStorer: IvoryScoobiStorer[Fact, DList[_]]): HdfsChord =
    copy(storer = newStorer)

  def run: ScoobiAction[Unit] = for {
    r  <- ScoobiAction.value(Repository.fromHdfsPath(repoPath))
    d  <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dictName))
    s  <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    es <- ScoobiAction.fromHdfs(Chord.readChords(entities))
    chordPath = new Path(tmpPath, java.util.UUID.randomUUID().toString)
    _  <- ScoobiAction.fromHdfs(Chord.serialiseChords(chordPath, es))
    _  <- scoobiJob(r, d, s, chordPath)
    _  <- storer.storeMeta
  } yield ()

  /**
   * Persist facts which are the latest corresponding to a set of dates given for each entity
   */
  def scoobiJob(repo: HdfsRepository, dict: Dictionary, store: FeatureStore, chordPath: Path): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      lazy val factsetMap = store.factSets.map(fs => (fs.priority.toShort, fs.name)).toMap

      factsFromIvoryStore(repo, store).map { input =>

        // DANGER - needs to be executed on mapper
        lazy val map: Mappings = Chord.deserialiseChords(chordPath).run(sc).run.unsafePerformIO() match {
          case Ok(m)    => m
          case Error(e) => sys.error("Can not deserialise chord map - " + Result.asString(e))
        }

        val errors: DList[String] = input.collect { case -\/(e) => e }

        // filter out the facts which are not in the entityMap or
        // which date are greater than the required dates for this entity
        val facts: DList[(Priority, Fact)] =
          input
            .collect { case \/-((p, _, fact)) => (p.toShort, fact) }
            .filter  { case (p, f) => DateMap.keep(map, f.entity, f.date.year, f.date.month, f.date.day) }

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
              fs.foldLeft(dates.map((_, Short.MinValue, None)): Array[(Int, Short, Option[Fact])]) { case (ds, (priority, fact)) =>
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
              }.collect { case (d, p, Some(f)) => (p, f.withEntity(f.entity + ":" + Date.fromInt(d).string())) }.toIterable
            }.collect { case (p, f) if !f.isTombstone => (p, f) }

        val validated: DList[String \/ Fact] = latest.map { case (p, f) =>
          Validate.validateFact(f, dict).disjunction.leftMap(_ + " - Factset " + factsetMap.get(p).getOrElse("Unknown, priority " + p))
        }

        val valErrors = validated.collect { case -\/(e) => e }
        val good      = validated.collect { case \/-(f) => f }

        persist(errors.toTextFile(new Path(errorPath, "parse").toString),
                valErrors.toTextFile(new Path(errorPath, "validation").toString),
                storer.storeScoobi(good))
        ()
      }
    }).flatten

}

object Chord {

  def onHdfs(repoPath: Path, store: String, dictName: String, entities: Path, output: Path, tmpPath: Path, errorPath: Path, storer: IvoryScoobiStorer[Fact, DList[_]]): ScoobiAction[Unit] =
    HdfsChord(repoPath, store, dictName, entities, tmpPath, errorPath, storer).run

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
