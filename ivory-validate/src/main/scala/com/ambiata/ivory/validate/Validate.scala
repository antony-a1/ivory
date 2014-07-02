package com.ambiata.ivory.validate

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import scalaz.Ordering._
import scalaz.effect._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.scoobi._, WireFormats._, FactFormats._
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.repository._

sealed trait Validate {
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

  def exec(output: Path): ScoobiAction[Long] = for {
    sc <- ScoobiAction.scoobiConfiguration
    j  <- scoobiJob
    _  <- ScoobiAction.safe(j.toTextFile(output.toString, overwrite = true).persist(sc))
  } yield getCounter(parseErrorCounterName)(sc) + getCounter(encodingErrorCounterName)(sc)

  def scoobiJob: ScoobiAction[DList[String]]
}

case class ValidateStoreHdfs(repo: HdfsRepository, store: FeatureStore, dict: Dictionary, includeOverridden: Boolean) extends Validate {
  def scoobiJob: ScoobiAction[DList[String]] =
    factsFromIvoryStore(repo, store).map(input => {
      val errors: DList[String] = countRecords(input.collect {
        case -\/(e) => e.message
      }, counterGroup, parseErrorCounterName)

      val facts: DList[(Priority, Factset, Fact)] = input.collect {
        case \/-(s) => s
      }

      // remove duplicates, taking the fact with the highest priority
      val reduced: DList[(Factset, Fact)] =
        if(!includeOverridden && store.factsets.size > 1) {
          val byKey = facts.map({ case (p, fs, f) => (f.coordinateString('|'), (p, fs, f)) }).groupByKey
          val ord: Order[(Priority, Factset, Fact)] = Order.orderBy({ case (p, _, _) => p })
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

case class ValidateFactSetHdfs(repo: HdfsRepository, factset: Factset, dict: Dictionary) extends Validate {

  def scoobiJob: ScoobiAction[DList[String]] =
    factsFromIvoryFactset(repo, factset).map(input => {
      val errors: DList[String] = countRecords(input.collect {
        case -\/(e) => e.message
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

  def validateHdfsStore(repoPath: Path, store: String, output: Path, includeOverridden: Boolean): ScoobiAction[Long] = for {
    r <- ScoobiAction.scoobiConfiguration.map(sc => Repository.fromHdfsPath(repoPath.toString.toFilePath, sc))
    d <- ScoobiAction.fromResultTIO(dictionaryFromIvory(r))
    s <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    c <- ValidateStoreHdfs(r, s, d, includeOverridden).exec(output)
  } yield c

  def validateHdfsFactSet(repoPath: Path, factset: Factset, output: Path): ScoobiAction[Long] = for {
    r <- ScoobiAction.scoobiConfiguration.map(sc => Repository.fromHdfsPath(repoPath.toString.toFilePath, sc))
    d <- ScoobiAction.fromResultTIO(dictionaryFromIvory(r))
    c <- ValidateFactSetHdfs(r, factset, d).exec(output)
  } yield c

  def validateFact(fact: Fact, dict: Dictionary): Validation[String, Fact] =
    dict.meta.get(fact.featureId).map(fm => validateEncoding(fact, fm.encoding)).getOrElse(s"Dictionary entry '${fact.featureId}' doesn't exist!".failure)

  def validateEncoding(fact: Fact, encoding: Encoding): Validation[String, Fact] = {
    val v = fact.value
    (encoding match {
      case BooleanEncoding => if(v.encoding.exists(_ != BooleanEncoding)) s"Not a valid boolean! '${fact.toString}'".failure else Success(())
      case IntEncoding     => if(v.encoding.exists(_ != IntEncoding)) s"Not a valid int! '${fact.toString}'".failure else Success(())
      case LongEncoding    => if(v.encoding.exists(_ != LongEncoding)) s"Not a valid long! '${fact.toString}'".failure else Success(())
      case DoubleEncoding  => if(v.encoding.exists(_ != DoubleEncoding)) s"Not a valid double! '${fact.toString}'".failure else Success(())
      case StringEncoding  => if(v.encoding.exists(_ != StringEncoding)) s"Not a valid string! '${fact.toString}'".failure else Success(())
    }).map(_ => fact)
  }
}
