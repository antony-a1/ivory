package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi._, WireFormats._, FactFormats._
import com.nicta.scoobi.Scoobi._
import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.parse.ListParser._
import org.joda.time.DateTimeZone

/**
 * This object provides functions to parse a text file containing files and
 * checked that they are indeed formatted as facts:
 *
 * entity|name|value|encoding|datetime
 */
object EavtTextStorageV1 {
  type Namespace = String

  case class EavtTextLoader(path: String, dict: Dictionary, namespace: String, timezone: DateTimeZone, preprocess: String => String) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
      fromTextFile(path).map(l => parseFact(dict, namespace, timezone, preprocess).run(splitLine(l)).leftMap(ParseError.withLine(l)).disjunction)
  }

  case class EavtTextStorer(base: String, delim: String = "|", tombstoneValue: Option[String] = None) extends IvoryScoobiStorer[Fact, DList[(Namespace, String)]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[(Namespace, String)] =
      dlist.mapFlatten(f =>
        DelimitedFactTextStorage.valueToString(f.value, tombstoneValue).map(v => (f.namespace, f.entity + delim + f.featureId.name + delim + v + delim + f.date.string("-")))
      ).toPartitionedTextFile(base, identity)

  }

  implicit class EavtTextFactStorageV1(dlist: DList[Fact]) {

    def toEavtTextFile(base: String, delim: String = "|", tombstoneValue: Option[String] = None)(implicit sc: ScoobiConfiguration): DList[(Namespace, String)] =
      EavtTextStorer(base, delim, tombstoneValue).storeScoobi(dlist)
  }

  def fromEavtTextFile(path: String, dict: Dictionary, namespace: String, timezone: DateTimeZone, preprocess: String => String)(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
    EavtTextLoader(path, dict, namespace, timezone, preprocess).loadScoobi

  def splitLine(line: String): List[String] =
    line.split('|').toList match {
      case e :: a :: v :: t :: Nil => List(e, a, v, t.trim)
      case other                   => other
    }

  def parseFact(dict: Dictionary, namespace: String, timezone: DateTimeZone, preprocessor: String => String): ListParser[Fact] = {
    import ListParser._
    for {
      entity <- string.nonempty
      name   <- string.nonempty
      rawv   <- string
      v      <- value(dict.meta.get(FeatureId(namespace, name)).map(fm => DelimitedFactTextStorage.valueFromString(fm, rawv)).getOrElse(s"Could not find dictionary entry for '$namespace.$name'".failure))
      time   <- either(localDatetime("yyyy-MM-dd HH:mm:ss"), localDate("yyyy-MM-dd")) // TODO replace with something that doesn't use joda
    } yield time match {
      case -\/(t) =>
        // FIX this looks wrong, it is getting the date with timezone, but millisOfDay without
        Fact.newFact(entity, namespace, name, Date.fromLocalDate(t.toDateTime(timezone).toLocalDate), Time.unsafe(t.getMillisOfDay / 1000), v)
      case \/-(t) =>
        Fact.newFact(entity, namespace, name, Date.fromLocalDate(t), Time(0), v)
    }
  }.preprocess(preprocessor)

  // FIX this probably belongs back in mundane.
  def either[A, B](x: ListParser[A], y: ListParser[B]): ListParser[A \/ B] =
    ListParser((n, ls) =>
      x.parse(n, ls) match {
        case Success((m, rest, a)) => Success((m, rest, a.left[B]))
        case Failure(_) => y.parse(n, ls).map(_.map(_.right[A]))
      })
}
