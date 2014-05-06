package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class FactSet(name: String, priority: Int) {
  def globPath: String =
    name + "/*/*/*/*/*"
}

object FactSets {
  def fromLines(lines: List[String]): String \/ List[FactSet] =
    lines.zipWithIndex.map { case (l, i) => (l, i + 1) } .map { case (l, i) =>
      val trimmed = l.trim
      if(trimmed.matches("\\s")) s"Line number $i '$l' contains a white space!".left else FactSet(l, i).right
    }.sequenceU

  def concat(init: List[FactSet], tail: List[FactSet]): List[FactSet] =
    (init ++ tail).zipWithIndex.map({ case (FactSet(n, _), p) => FactSet(n, p) })

  def diff(factsets: List[FactSet], other: List[FactSet]): List[FactSet] =
    factsets.map(_.name).diff(other.map(_.name)).zipWithIndex.map({ case (n, p) => FactSet(n, p) })
}
