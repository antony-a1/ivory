package com.ambiata.ivory.core

import scalaz._, Scalaz._
import java.io.File
import org.joda.time.LocalDate
import com.ambiata.mundane.parse.ListParser

object Partition {

  type FactSetName = String
  type Namespace = String

  def parseFilename(file: File): Validation[String, (FactSetName, Namespace, LocalDate)] =
    parseWith(file.toURI.getPath)

  def parseWith(f: => String): Validation[String, (FactSetName, Namespace, LocalDate)] =
    pathParser.run(f.split("/").toList.reverse)

  def pathParser: ListParser[(FactSetName, Namespace, LocalDate)] = {
    import ListParser._
    for {
      _       <- consume(1)
      day     <- int
      month   <- int
      year    <- int
      ns      <- string
      factset <- string
      _       <- consumeRest
    } yield (factset, ns, new LocalDate(year, month, day))
  }

  def path(ns: Namespace, date: LocalDate): String =
    ns + "/" + date.toString("yyyy/MM/dd")
}
