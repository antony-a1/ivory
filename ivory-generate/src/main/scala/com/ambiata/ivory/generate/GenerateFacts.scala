package com.ambiata.ivory.generate

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import com.nicta.rng._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.joda.time.{LocalDate, Days, Weeks, Months}
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import java.util.Random

import com.ambiata.ivory.core.{Value => IValue, _}
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._

case class HdfsGenerateFacts(repository: Repository, entities: Int, flags: Path, start: LocalDate, end: LocalDate, storer: IvoryScoobiStorer[Fact, DList[_]]) {
  def withStorer(newStorer: IvoryScoobiStorer[Fact, DList[_]]): HdfsGenerateFacts =
    copy(storer = newStorer)

  def run(implicit sc: ScoobiConfiguration): Hdfs[Unit] = for {
    dict <- Hdfs.fromResultTIO(IvoryStorage.dictionaryFromIvory(repository))
    fl   <- FeatureFlags.fromHdfs(flags)
  } yield scoobiJob(dict, fl)

  def scoobiJob(dict: Dictionary, flags: List[FeatureFlags])(implicit sc: ScoobiConfiguration) {
    // average 1m data points per mapper
    val dataPointsPerMapper = 1000000
    val mappers = math.max(((entities * factsPerEntity(flags)) / dataPointsPerMapper).toInt, 1)

    // set parallelism factor
    sc.set("mapred.map.tasks", mappers)
    println(s"-------------- number of mappers: $mappers")
    val facts: DList[Fact] = DList.tabulate(entities)(i => RandomFacts(new Random()).facts(i, dict, flags, start, end)).mapFlatten(identity)

    storer.storeScoobi(facts).persist
  }

  def factsPerEntity(flags: List[FeatureFlags]): Double =
    flags.foldLeft(0.0)((s, ff) => {
      val f = ff.frequency match {
        case Monthly() => Months.monthsBetween(start, end).getMonths
        case Weekly()  => Weeks.weeksBetween(start, end).getWeeks
        case Daily()   => Days.daysBetween(start, end).getDays
      }
      s + (f.toDouble * (1.0 - ff.sparcity))
    })
}

object GenerateFacts {
  import DelimitedFactTextStorage._

  def onHdfs(repository: Repository, entities: Int, flags: Path, start: LocalDate, end: LocalDate, output: Path)(implicit sc: ScoobiConfiguration): Hdfs[Unit] =
    HdfsGenerateFacts(repository, entities, flags, start, end, DelimitedFactTextStorer(output)).run

}

case class RandomFacts(rand: Random) {

  def facts(eid: Int, dict: Dictionary, flags: List[FeatureFlags], start: LocalDate, end: LocalDate): Stream[Fact] = {
    def datesBy(incr: (Int, LocalDate) => LocalDate): Stream[LocalDate] =
      Stream.tabulate(Int.MaxValue)(i => incr(i, start)).takeWhile(d => d.isBefore(end))

    def createFact(ff: FeatureFlags, d: LocalDate): Option[Fact] = {
      val fid = FeatureId(ff.namespace, ff.name)
      if(rand.nextDouble() > ff.sparcity) dict.meta.get(fid).map(m => fact(eid, fid, m, d)) else None
    }

    flags.toStream.flatMap(ff => ff.frequency match {
      case _ if(end.isBefore(start)) => Stream((ff, start))
      case Monthly()                 => datesBy((i, d) => d.plusMonths(i)).map((ff, _))
      case Weekly()                  => datesBy((i, d) => d.plusWeeks(i)).map((ff, _))
      case Daily()                   => datesBy((i, d) => d.plusDays(i)).map((ff, _))
    }).flatMap({ case (ff, d) => createFact(ff, d) })
  }

  def fact(eid: Int, fid: FeatureId, meta: FeatureMeta, date: LocalDate): Fact =
    Fact.newFact("ID%08d".format(eid), fid.namespace, fid.name, Date.fromLocalDate(date), Time.unsafe(rand.nextInt(86400)), value(meta))

  def value(meta: FeatureMeta): IValue = meta match {
    case FeatureMeta(BooleanEncoding, _, _, _)   => BooleanValue(rand.nextBoolean)
    case FeatureMeta(IntEncoding, _, _, _)       => IntValue(rand.nextInt)
    case FeatureMeta(LongEncoding, _, _, _)      => LongValue(rand.nextLong)
    case FeatureMeta(DoubleEncoding, _, _, _)    => DoubleValue(rand.nextDouble)
    case FeatureMeta(StringEncoding, _, _, _)    => StringValue(org.apache.commons.lang.RandomStringUtils.randomAlphanumeric(10))
  }
}
