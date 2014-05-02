package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.repository._
import com.nicta.scoobi._, Scoobi._

import scalaz.{DList => _, _}, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

sealed trait IvorySource {
  def toDList: DList[(Int, Fact)]
//  def toProcess: Process[Task, (Priority, Fact)]
}

object IvorySource {
  def fromEavt(path: String): IvorySource =
    ???

  // FIX MTH up to here ............ depends on some other refactoring to get Versions, Repositories and Locations under control.
  def fromRepository(repository: Repository, store: FeatureStore): ScoobiAction[IvorySource] = ??? /*for {
        sc       <- ScoobiAction.scoobiConfiguration
        versions <- store.factSets.traverseU(factset => ScoobiAction.fromHdfs(Versions.readFactsetVersionFromHdfs(repository, factset.name).map((factset, _))))
  } yield new IvorySource {
      def toDList: DList[(Priority, Fact)] =
        versions
          .groupBy(_._2).toList
          .map({ case (k, vs) => (k, vs.map(_._1)) })
          .map({ case (v, fss) =>
            IvoryStorage.multiFactsetLoader(v, repo.factsetsPath, fss).loadScoobi(sc)
           }).reduce(_++_).map({ case (p, n, f) => (p, f) })
  }*/

  def fromThriftExtract(path: String): IvorySource =
    ???
}
