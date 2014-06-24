package com.ambiata.ivory.storage.parse

import com.ambiata.ivory.core._
import com.ambiata.mundane.parse._, ListParser._

import org.joda.time.DateTimeZone

import scalaz.{Value => _, _}, Scalaz._

// FIX should be in storage.fact
object EavtParsers {
  def splitLine(line: String): List[String] =
    line.split('|').toList match {
      case e :: a :: v :: t :: Nil => List(e, a, v, t.trim)
      case other                   => other
    }

  def parse(line: String, dictionary: Dictionary, namespace: String, timezone: DateTimeZone): Validation[String, Fact] =
    fact(dictionary, namespace, timezone).run(splitLine(line))

  def fact(dictionary: Dictionary, namespace: String, timezone: DateTimeZone): ListParser[Fact] =
    for {
      entity <- string.nonempty
      name   <- string.nonempty
      rawv   <- string
      v      <- value(dictionary.meta.get(FeatureId(namespace, name)).map(fm => valueFromString(fm, rawv)).getOrElse(s"Could not find dictionary entry for '$namespace.$name'".failure))
      time   <- Dates.parser(timezone, timezone)
    } yield time match {
      case \/-(dt) =>
        Fact.newFact(entity, namespace, name, dt.date, dt.time, v)
      case -\/(d) =>
        Fact.newFact(entity, namespace, name, d, Time(0), v)
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
