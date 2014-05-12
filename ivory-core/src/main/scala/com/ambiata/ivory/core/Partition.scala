package com.ambiata.ivory.core

import scalaz._, Scalaz._
import java.io.File
import com.ambiata.mundane.parse.ListParser

case class Partition(factset: String, namespace: String, date: Date, base: Option[String] = None) {

  lazy val path: String =
    base.map(_ + "/").getOrElse("") + factset + "/" + namespace + "/" + "%4d/%02d/%02d".format(date.year, date.month, date.day)
}

object Partition {

  type Namespace = String
  type Base = String

  def parseFilename(file: File): Validation[String, Partition] =
    parseWith(file.toURI.getPath)

  def parseWith(f: => String): Validation[String, Partition] =
    pathParser.run(f.split("/").toList.reverse)

  def pathParser: ListParser[Partition] = {
    import ListParser._
    for {
      _       <- consume(1)
      d        <- short
      m        <- short
      y        <- short
      date     <- Date.create(y, m.toByte, d.toByte) match {
        case None => ListParser((position, _) => (position, s"""not a valid date ($y-$m-$d)""").failure)
        case Some(d) => d.point[ListParser]
      }
      ns      <- string
      factset <- string
      rest    <- ListParser((pos, str) => (str.length, Nil, str.mkString("/")).success)
    } yield Partition(factset, ns, date, Some(rest))
  }

  def path(ns: Namespace, date: Date): String = {
    ns + "/" + "%4d/%02d/%02d".format(date.year, date.month, date.day)
  }
}

object Partitions {

  /** Filter paths before or equal to a given date */
  def pathsBeforeOrEqual(partitions: List[Partition], to: Date): List[Partition] =
    partitions.filter(_.date.isBeforeOrEqual(to))

  /** Filter paths after or equal to a given date */
  def pathsAfterOrEqual(partitions: List[Partition], from: Date): List[Partition] =
    partitions.filter(_.date.isAfterOrEqual(from))

  /** Filter paths between two dates (inclusive) */
  def pathsBetween(partitions: List[Partition], from: Date, to: Date): List[Partition] =
    pathsBeforeOrEqual(pathsAfterOrEqual(partitions, from), to)
}
