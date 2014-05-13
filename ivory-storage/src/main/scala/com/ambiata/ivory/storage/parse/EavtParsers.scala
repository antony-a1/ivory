package com.ambiata.ivory.storage.parse

import com.ambiata.ivory.core._
import com.ambiata.mundane.parse._, ListParser._

import org.joda.time.DateTimeZone

import scalaz.{Value => _, _}, Scalaz._

object EavtParsers {
  def splitLine(line: String): List[String] =
    line.split('|').toList match {
      case e :: a :: v :: t :: Nil => List(e, a, v, t.trim)
      case other                   => other
    }

  def fact(dictionary: Dictionary, namespace: String, timezone: DateTimeZone): ListParser[Fact] =
    for {
      entity <- string.nonempty
      name   <- string.nonempty
      rawv   <- string
      v      <- value(dictionary.meta.get(FeatureId(namespace, name)).map(fm => valueFromString(fm, rawv)).getOrElse(s"Could not find dictionary entry for '$namespace.$name'".failure))
      // FIX support proper time format
      time   <- either(localDatetime("yyyy-MM-dd HH:mm:ss"), localDate("yyyy-MM-dd")) // TODO replace with something that doesn't use joda
    } yield time match {
      case -\/(t) =>
        // FIX this looks wrong, it is getting the date with timezone, but millisOfDay without
        Fact.newFact(entity, namespace, name, Date.fromLocalDate(t.toDateTime(timezone).toLocalDate), Time.unsafe(t.getMillisOfDay / 1000), v)
      case \/-(t) =>
        Fact.newFact(entity, namespace, name, Date.fromLocalDate(t), Time(0), v)
    }

  // FIX this probably belongs back in mundane.
  def either[A, B](x: ListParser[A], y: ListParser[B]): ListParser[A \/ B] =
    ListParser((n, ls) =>
      x.parse(n, ls) match {
        case Success((m, rest, a)) => Success((m, rest, a.left[B]))
        case Failure(_) => y.parse(n, ls).map(_.map(_.right[A]))
      })

  def valueFromString(meta: FeatureMeta, raw: String): Validation[String, Value] = meta.encoding match {
    case _ if(meta.tombstoneValue.contains(raw)) => TombstoneValue().success[String]
    case BooleanEncoding                         => raw.parseBoolean.leftMap(_ => s"Value '$raw' is not a boolean").map(v => BooleanValue(v))
    case IntEncoding                             => raw.parseInt.leftMap(_ => s"Value '$raw' is not an integer").map(v => IntValue(v))
    case LongEncoding                            => raw.parseLong.leftMap(_ => s"Value '$raw' is not a long").map(v => LongValue(v))
    case DoubleEncoding                          => raw.parseDouble.leftMap(_ => s"Value '$raw' is not a double").map(v => DoubleValue(v))
    case StringEncoding                          => StringValue(raw).success[String]
  }
}
