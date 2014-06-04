package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi._, WireFormats._, FactFormats._
import com.ambiata.ivory.storage.parse._
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
    EavtParsers.splitLine(line)

  def parseFact(dict: Dictionary, namespace: String, timezone: DateTimeZone, preprocessor: String => String): ListParser[Fact] =
    EavtParsers.fact(dict, namespace, timezone).preprocess(preprocessor)
}
