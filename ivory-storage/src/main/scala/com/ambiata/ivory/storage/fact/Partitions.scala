package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import scalaz._, Scalaz._, effect.IO, \&/._

object Partitions {
  def select(repository: Repository, factset: Factset): ResultT[IO, List[Partition]] = for {
    paths <- repository.toStore.list(Repository.factset(factset))
    parts <- paths.map(_.basename).distinct.traverseU(parse)
  } yield parts

  def before(repository: Repository, factset: Factset, to: Date): ResultT[IO, List[Partition]] =
    select(repository, factset).map(_.filter(_.date.isBeforeOrEqual(to)))

  def after(repository: Repository, factset: Factset, from: Date): ResultT[IO, List[Partition]] =
    select(repository, factset).map(_.filter(_.date.isAfterOrEqual(from)))

  def between(repository: Repository, factset: Factset, from: Date, to: Date): ResultT[IO, List[Partition]] =
    select(repository, factset).map(_.filter(p => p.date.isBeforeOrEqual(to) && p.date.isAfterOrEqual(from)))

  def parse(f: FilePath): ResultT[IO, Partition] =
    ResultT.fromDisjunction(Partition.parseWith(f.path).disjunction.leftMap(This.apply))
}
