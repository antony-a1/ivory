package com.ambiata.ivory.storage

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.WireFormats._
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
object EavtTextStorage {
  type Namespace = String

  case class EavtTextLoaderV1(path: String, dict: Dictionary, namespace: String, timezone: DateTimeZone, preprocess: String => String) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[String \/ Fact] =
      fromTextFile(path).map(l => parseFact(dict, namespace, timezone, preprocess).run(splitLine(l)).disjunction)
  }

  case class EavtTextStorerV1(base: String, delim: String = "|", tombstoneValue: Option[String] = None) extends IvoryScoobiStorer[Fact, DList[(Namespace, String)]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[(Namespace, String)] =
      dlist.mapFlatten(f =>
        DelimitedFactTextStorage.valueToString(f.value, tombstoneValue).map(v => (f.featureId.namespace, f.entity + delim + f.featureId.name + delim + v + delim + f.date.toString("yyyy-MM-dd")))
      ).toPartitionedTextFile(base, identity)

  }

  implicit class EavtTextFactStorage(dlist: DList[Fact]) {

    def toEavtTextFile(base: String, delim: String = "|", tombstoneValue: Option[String] = None)(implicit sc: ScoobiConfiguration): DList[(Namespace, String)] =
      EavtTextStorerV1(base, delim, tombstoneValue).storeScoobi(dlist)
  }

  def fromEavtTextFile(path: String, dict: Dictionary, namespace: String, timezone: DateTimeZone, preprocess: String => String)(implicit sc: ScoobiConfiguration): DList[String \/ Fact] =
    EavtTextLoaderV1(path, dict, namespace, timezone, preprocess).loadScoobi

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
      fid     = FeatureId(namespace, name)
      rawv   <- string
      v      <- value(dict.meta.get(fid).map(fm => DelimitedFactTextStorage.valueFromString(fm, rawv)).getOrElse(s"Could not find dictionary entry for '$fid'".failure))
      time   <- localDatetime("yyyy-MM-dd HH:mm:ss")
    } yield Fact(entity, fid, time.toDateTime(timezone).toLocalDate, time.getMillisOfDay / 1000, v)
  }.preprocess(preprocessor)
}

