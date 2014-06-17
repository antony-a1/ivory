package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import scalaz._, Scalaz._, effect.IO, \&/._

object StoreGlob {
  def select(repository: Repository, store: FeatureStore): ResultT[IO, List[FactsetGlob]] = for {
    globs <- store.factsets.traverseU(factset => for {
      fv <- Versions.read(repository, factset.set)
      ps <- FactsetGlob.select(repository, factset.set)
    } yield FactsetGlob(fv, List((factset, ps))))
  } yield FactsetGlob.groupByVersion(globs)

  def before(repository: Repository, store: FeatureStore, to: Date): ResultT[IO, List[FactsetGlob]] =
    select(repository, store).map(_.map(_.filterByPartition(_.date.isBeforeOrEqual(to))).filter(!_.factsets.isEmpty))

  def after(repository: Repository, store: FeatureStore, from: Date): ResultT[IO, List[FactsetGlob]] =
    select(repository, store).map(_.map(_.filterByPartition(_.date.isAfterOrEqual(from))).filter(!_.factsets.isEmpty))

  def between(repository: Repository, store: FeatureStore, from: Date, to: Date): ResultT[IO, List[FactsetGlob]] =
    select(repository, store).map(_.map(_.filterByPartition(p => p.date.isBeforeOrEqual(to) && p.date.isAfterOrEqual(from))).filter(!_.factsets.isEmpty))
}
