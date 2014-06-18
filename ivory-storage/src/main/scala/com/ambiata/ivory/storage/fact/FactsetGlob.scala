package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import scalaz._, Scalaz._, effect.IO, \&/._

case class FactsetGlob(version: FactsetVersion, factsets: List[(PrioritizedFactset, List[Partition])]) {
  def filterByPartition(f: Partition => Boolean): FactsetGlob =
    copy(factsets = factsets.map({ case (pf, ps) => (pf, ps.filter(f)) }).filter(!_._2.isEmpty))

  def partitions: List[Partition] =
    factsets.flatMap(_._2)
}

object FactsetGlob {
  def select(repository: Repository, factset: Factset): ResultT[IO, List[Partition]] = for {
    paths <- repository.toStore.list(Repository.factset(factset) </> "/*/*/*/*")
    parts <- paths.traverseU(path => ResultT.fromDisjunction[IO, Partition](Partition.parseWith((repository.root </> path).path).disjunction.leftMap(This.apply)))
  } yield parts

  def before(repository: Repository, factset: Factset, to: Date): ResultT[IO, List[Partition]] =
    select(repository, factset).map(_.filter(_.date.isBeforeOrEqual(to)))

  def after(repository: Repository, factset: Factset, from: Date): ResultT[IO, List[Partition]] =
    select(repository, factset).map(_.filter(_.date.isAfterOrEqual(from)))

  def between(repository: Repository, factset: Factset, from: Date, to: Date): ResultT[IO, List[Partition]] =
    select(repository, factset).map(_.filter(p => p.date.isBeforeOrEqual(to) && p.date.isAfterOrEqual(from)))

  def groupByVersion(globs: List[FactsetGlob]): List[FactsetGlob] =
    globs.groupBy(_.version).toList.map({ case (v, gs) =>
      FactsetGlob(v, gs.flatMap(_.factsets))
    })
}
