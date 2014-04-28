package com.ambiata.ivory.storage

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.joda.time.LocalDateTime
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.WireFormats._

object DelimitedFactTextStorage {
  
  case class DelimitedFactTextLoader(path: String, dict: Dictionary) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[String \/ Fact] = {
      fromTextFile(path).map(line => parseFact(dict, line))
    }
  }

  case class DelimitedFactTextStorer(path: Path, delim: String = "|", tombstoneValue: Option[String] = Some("☠")) extends IvoryScoobiStorer[Fact, DList[String]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[String] =
    dlist.mapFlatten(f =>
      valueToString(f.value, tombstoneValue).map(v => f.entity + delim + f.featureId.namespace + ":" + f.featureId.name + delim + v + delim + time(f.date, f.seconds).toString("yyyy-MM-dd HH:mm:ss"))
    ).toTextFile(path.toString)

    def time(d: Date, s: Int): LocalDateTime =
      d.localDate.toDateTimeAtStartOfDay.toLocalDateTime.plusSeconds(s)
  }

  def parseFact(dict: Dictionary, str: String): String \/ Fact =
    factParser(dict).run(str.split('|').toList).disjunction

  def factParser(dict: Dictionary): ListParser[Fact] = {
    import ListParser._
    for {
      entity <- string.nonempty
      attr   <- string.nonempty
      fid    <- value(featureIdParser.run(attr.split(":", 2).toList))
      rawv   <- string
      v      <- value(dict.meta.get(fid).map(fm => valueFromString(fm, rawv)).getOrElse(s"Could not find dictionary entry for '${fid}'".failure))
      date   <- localDatetime("yyyy-MM-dd HH:mm:ss") // TODO replace with something that doesn't convert to joda
    } yield Fact.newFact(entity, fid, Date.fromLocalDate(date.toLocalDate), date.getMillisOfDay / 1000, v)
  }

  def featureIdParser: ListParser[FeatureId] = {
    import ListParser._
    for {
      ns   <- string.nonempty
      name <- string.nonempty
    } yield FeatureId(ns, name)
  }

  def valueToString(v: Value, tombstoneValue: Option[String]): Option[String] = v match {
    case BooleanValue(b)  => Some(b.toString)
    case IntValue(i)      => Some(i.toString)
    case LongValue(i)     => Some(i.toString)
    case DoubleValue(d)   => Some(d.toString)
    case StringValue(s)   => Some(s)
    case TombstoneValue() => tombstoneValue
  }

  def valueFromString(meta: FeatureMeta, raw: String): Validation[String, Value] = meta.encoding match {
    case _ if(meta.tombstoneValue.contains(raw)) => TombstoneValue().success[String]
    case BooleanEncoding                         => raw.parseBoolean.leftMap(_ => s"Value '$raw' is not a boolean").map(v => BooleanValue(v))
    case IntEncoding                             => raw.parseInt.leftMap(_ => s"Value '$raw' is not an integer").map(v => IntValue(v))
    case LongEncoding                            => raw.parseLong.leftMap(_ => s"Value '$raw' is not a long").map(v => LongValue(v))
    case DoubleEncoding                          => raw.parseDouble.leftMap(_ => s"Value '$raw' is not a double").map(v => DoubleValue(v))
    case StringEncoding                          => StringValue(raw).success[String]
  }
}

