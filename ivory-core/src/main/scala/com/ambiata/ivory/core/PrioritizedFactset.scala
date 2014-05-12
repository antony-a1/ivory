package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class PrioritizedFactset(set: Factset, priority: Int) {
  def globPath: String =
    set.name + "/*/*/*/*/*"
}

object PrioritizedFactset {
  def fromFactsets(sets: List[Factset]): List[PrioritizedFactset] =
    sets.zipWithIndex.map({ case (set, i) => PrioritizedFactset(set, i) })

  def fromLines(lines: List[String]): String \/ List[PrioritizedFactset] =
    lines.zipWithIndex.map { case (l, i) => (l, i + 1) } .map { case (l, i) =>
      val trimmed = l.trim
      if(trimmed.matches("\\s")) s"Line number $i '$l' contains white space.".left else PrioritizedFactset(Factset(l), i).right
    }.sequenceU

  def concat(init: List[PrioritizedFactset], tail: List[PrioritizedFactset]): List[PrioritizedFactset] =
    (init ++ tail).zipWithIndex.map({ case (PrioritizedFactset(n, _), p) => PrioritizedFactset(n, p) })

  def diff(factsets: List[PrioritizedFactset], other: List[PrioritizedFactset]): List[PrioritizedFactset] =
    factsets.map(_.set.name).diff(other.map(_.set.name)).zipWithIndex.map({ case (n, p) => PrioritizedFactset(Factset(n), p) })
}
