package com.ambiata.ivory.storage.fact

sealed abstract class FactsetVersion(override val toString: String)
case object FactsetVersionOne extends FactsetVersion("1")
case object FactsetVersionTwo extends FactsetVersion("2")

object FactsetVersion {
  def fromString(str: String): Option[FactsetVersion] = str match {
    case "1" => Some(FactsetVersionOne)
    case "2" => Some(FactsetVersionTwo)
    case _   => None
  }
}
