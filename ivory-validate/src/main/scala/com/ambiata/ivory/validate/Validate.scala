package com.ambiata.ivory.validate

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import scalaz.Ordering._
import scalaz.effect._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.scoobi.WireFormats, WireFormats._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage.IvoryStorage._


sealed trait Validate {
  type Priority = Int
  type FactSetName = String

  val counterGroup = "VALIDATION"
  val parseErrorCounterName = "PARSE_ERRORS"
  val encodingErrorCounterName = "ENCODING_ERRORS"

  def countRecords[A: WireFormat](dlist: DList[A], group: String, name: String): DList[A] =
    dlist.parallelDo((input: A, counters: Counters) => {
      counters.incrementCounter(group, name, 1)
      input
    })

  def getCounter(name: String)(implicit sc: ScoobiConfiguration): Long =
    (for {
      grp     <- Option(sc.counters.getGroup(counterGroup))
      counter <- Option(grp.findCounter(name))
    } yield counter.getValue).getOrElse(0)

  def exec(output: Path)(implicit sc: ScoobiConfiguration): ScoobiAction[Long] = for {
    j <- scoobiJob
    _ <- ScoobiAction.safe(j.toTextFile(output.toString).persist)
  } yield getCounter(parseErrorCounterName) + getCounter(encodingErrorCounterName)

  def scoobiJob: ScoobiAction[DList[String]]
}

case class ValidateStoreHdfs(repo: HdfsRepository, store: FeatureStore, dict: Dictionary, includeOverridden: Boolean)(implicit sc: ScoobiConfiguration) extends Validate {
  implicit val FactWireFormat = WireFormats.FactWireFormat

  def scoobiJob: ScoobiAction[DList[String]] =
    factsFromIvoryStore(repo, store).map(input => {
      val errors: DList[String] = countRecords(input.collect {
        case -\/(e) => e
      }, counterGroup, parseErrorCounterName)

      val facts: DList[(Priority, FactSetName, Fact)] = input.collect {
        case \/-(s) => s
      }

      // remove duplicates, taking the fact with the highest priority
      val reduced: DList[(FactSetName, Fact)] =
        if(!includeOverridden && store.factSets.size > 1) {
          val byKey = facts.map({ case (p, fs, f) => (f.coordinateString('|'), (p, fs, f)) }).groupByKey
          val ord: Order[(Priority, FactSetName, Fact)] = Order.orderBy({ case (p, _, _) => p })
          byKey.reduceValues(Reduction.minimum(ord)).map({ case (_, (p, fs, f)) => (fs, f) })
        } else {
          facts.map({ case (_, fs, f) => (fs, f) })
        }

      val validated: DList[Validation[String, Fact]] =
        reduced.map({ case (fs, f) =>
          dict.meta.get(f.featureId).map(fm =>
            Validate.validateFact(f, dict).leftMap(e => s"${e} - Fact set '${fs}'")
          ).getOrElse(s"Dictionary entry '${f.featureId}' doesn't exist!".failure)
        })

      val validationErrors: DList[String] = countRecords(validated.collect {
        case Failure(e) => e
      }, counterGroup, encodingErrorCounterName)

      errors ++ validationErrors
    })
}

case class ValidateFactSetHdfs(repo: HdfsRepository, factset: String, dict: Dictionary) extends Validate {

  def scoobiJob: ScoobiAction[DList[String]] =
    factsFromIvoryFactset(repo, factset).map(input => {
      implicit val FactWireFormat = WireFormats.FactWireFormat

      val errors: DList[String] = countRecords(input.collect {
        case -\/(e) => e
      }, counterGroup, parseErrorCounterName)

      val facts: DList[Fact] = input.collect {
        case \/-(s) => s
      }

      val validated: DList[Validation[String, Fact]] = facts.map(f => Validate.validateFact(f, dict))

      val validationErrors: DList[String] = countRecords(validated.collect {
        case Failure(e) => e
      }, counterGroup, encodingErrorCounterName)

      errors ++ validationErrors
    })
}

object Validate {

  def validateHdfsStore(repoPath: Path, store: String, dict: String, output: Path, includeOverridden: Boolean)(implicit sc: ScoobiConfiguration): ScoobiAction[Long] = for {
    r <- ScoobiAction.value(Repository.fromHdfsPath(repoPath))
    d <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dict))
    s <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    c <- ValidateStoreHdfs(r, s, d, includeOverridden).exec(output)
  } yield c

  def validateHdfsFactSet(repoPath: Path, factset: String, dict: String, output: Path)(implicit sc: ScoobiConfiguration): ScoobiAction[Long] = for {
    r <- ScoobiAction.value(Repository.fromHdfsPath(repoPath))
    d <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dict))
    c <- ValidateFactSetHdfs(r, factset, d).exec(output)
  } yield c

  def validateFact(fact: Fact, dict: Dictionary): Validation[String, Fact] =
    dict.meta.get(fact.featureId).map(fm => validateEncoding(fact, fm.encoding)).getOrElse(s"Dictionary entry '${fact.featureId}' doesn't exist!".failure)

  def validateEncoding(fact: Fact, encoding: Encoding): Validation[String, Fact] = {
    val v = fact.value
    (encoding match {
      case BooleanEncoding => if(v.encoding != BooleanEncoding) s"Not a valid boolean! '${fact.toString}'".failure else Success(())
      case IntEncoding     => if(v.encoding != IntEncoding) s"Not a valid int! '${fact.toString}'".failure else Success(())
      case LongEncoding    => if(v.encoding != LongEncoding) s"Not a valid long! '${fact.toString}'".failure else Success(())
      case DoubleEncoding  => if(v.encoding != DoubleEncoding) s"Not a valid double! '${fact.toString}'".failure else Success(())
      case StringEncoding  => if(v.encoding != StringEncoding) s"Not a valid string! '${fact.toString}'".failure else Success(())
      case TombstoneEncoding => if(v.encoding != TombstoneEncoding) s"Not a valid tombstone! '${fact.toString}'".failure else Success(())
    }).map(_ => fact)
  }
}
