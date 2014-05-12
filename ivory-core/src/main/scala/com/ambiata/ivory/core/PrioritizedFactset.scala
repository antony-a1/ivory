package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class PrioritizedFactset(name: String, priority: Int) {
  def globPath: String =
    name + "/*/*/*/*/*"
}

object PrioritizedFactset {
  def fromLines(lines: List[String]): String \/ List[PrioritizedFactset] =
    lines.zipWithIndex.map { case (l, i) => (l, i + 1) } .map { case (l, i) =>
      val trimmed = l.trim
      if(trimmed.matches("\\s")) s"Line number $i '$l' contains a white space!".left else PrioritizedFactset(l, i).right
    }.sequenceU

  def concat(init: List[PrioritizedFactset], tail: List[PrioritizedFactset]): List[PrioritizedFactset] =
    (init ++ tail).zipWithIndex.map({ case (PrioritizedFactset(n, _), p) => PrioritizedFactset(n, p) })

  def diff(factsets: List[PrioritizedFactset], other: List[PrioritizedFactset]): List[PrioritizedFactset] =
    factsets.map(_.name).diff(other.map(_.name)).zipWithIndex.map({ case (n, p) => PrioritizedFactset(n, p) })
}
