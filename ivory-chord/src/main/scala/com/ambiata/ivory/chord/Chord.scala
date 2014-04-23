package com.ambiata.ivory.chord

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import org.joda.time.{LocalDate, LocalDateTime, LocalTime}
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.mundane.time.DateTimex
import com.ambiata.mundane.parse._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage._
import com.ambiata.ivory.validate.Validate
import com.ambiata.ivory.alien.hdfs._

case class HdfsChord(repoPath: Path, store: String, dictName: String, entities: Path, errorPath: Path, storer: IvoryScoobiStorer[Fact, DList[_]]) {
  import IvoryStorage._

  type Priority = Short

  def withStorer(newStorer: IvoryScoobiStorer[Fact, DList[_]]): HdfsChord =
    copy(storer = newStorer)

  def run: ScoobiAction[Unit] = for {
    r  <- ScoobiAction.value(Repository.fromHdfsPath(repoPath))
    d  <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dictName))
    s  <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    es <- ScoobiAction.fromHdfs(readEntities(entities))
    _  <- scoobiJob(r, d, s, es)
    _  <- storer.storeMeta
  } yield ()

  def readEntities(path: Path): Hdfs[Map[String, Array[Int]]] = for {
    lines <- Hdfs.readWith(path, is => Streams.read(is)).map(_.lines.toList)
    map   <- Hdfs.fromDisjunction(parseLines(lines))
  } yield map

  def parseLines(lines: List[String]): String \/ Map[String, Array[Int]] =
    lines.traverseU(l => Chord.entityParser.run(Delimited.parsePsv(l)).disjunction).map(entries => {
      entries.groupBy(_._1).map({ case (e, ts) => (e, ts.map(_._2).sortBy(-_).toArray) })
    })

  def scoobiJob(repo: HdfsRepository, dict: Dictionary, store: FeatureStore, entities: Map[String, Array[Int]]): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      lazy val factsetMap = store.factSets.map(fs => (fs.priority.toShort, fs.name)).toMap
      factsFromIvoryStore(repo, store).map(input => {
        val entityMap = DObject(entities)

        val errors: DList[String] = input.collect {
          case -\/(e) => e
        }

        val facts: DList[(Priority, Fact)] = (entityMap join input).mapFlatten({
          case (es, \/-((p, _, f))) =>
            val times = es.getOrElse(f.entity, Array())
            times.flatMap(epoch => {
              val ld = new LocalDate(epoch.toLong * 1000)
              if(f.date.isBefore(ld)) Some((p.toShort, f.copy(entity = f.entity + ":" + ld.toString("yyyy-MM-dd")))) else None
            })
          case _                    => Nil
        })

        /*
         * 1. group by entity and feature id
         * 2. take the minimum fact in the group using fact time then priority to determine order
         */
        implicit val revDateOrder: Order[LocalDateTime] = DateTimex.LocalDateTimeHasOrder.reverseOrder
        val ord: Order[(Priority, Fact)] = Order.orderBy { case (p, f) => (f.time, p) }
        val latest: DList[(Priority, Fact)] = facts.groupBy { case (p, f) => (f.entity, f.featureId.toString) }
                                                   .reduceValues(Reduction.minimum(ord))
                                                   .collect { case (_, (p, f)) if !f.isTombstone => (p, f) }

        val validated: DList[String \/ Fact] = latest.map({ case (p, f) =>
          Validate.validateFact(f, dict).disjunction.leftMap(e => e + " - Factset " + factsetMap.get(p).getOrElse("Unknown, priority " + p))
        })

        val valErrors = validated.collect {
          case -\/(e) => e
        }

        val good = validated.collect {
          case \/-(f) => f
        }

        persist(errors.toTextFile((new Path(errorPath, "parse")).toString),
                valErrors.toTextFile((new Path(errorPath, "validation")).toString),
                storer.storeScoobi(good))(sc)
        ()
      })
    }).flatten
}

object Chord {

  def onHdfs(repoPath: Path, store: String, dictName: String, entities: Path, output: Path, errorPath: Path, storer: IvoryScoobiStorer[Fact, DList[_]]): ScoobiAction[Unit] =
    HdfsChord(repoPath, store, dictName, entities, errorPath, storer).run

  def entityParser: ListParser[(String, Int)] = {
    import ListParser._
    for {
      e <- string.nonempty
      d <- localDate
    } yield (e, (d.toDateTime(new LocalTime(0)).getMillis / 1000).toInt)
  }
}
