package com.ambiata.ivory.storage.fact

import com.ambiata.mundane.control._
import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.scoobi._, FactFormats._
import com.ambiata.ivory.storage.repository._
import com.nicta.scoobi._, Scoobi._

import scalaz.{DList => _, _}, Scalaz._
import scalaz.concurrent._
import scalaz.effect._
import scalaz.stream._

sealed trait IvorySource {
  def toDList: DList[(Priority, Fact)]
//  def toProcess: Process[Task, (Priority, Fact)]
}

object IvorySource {
  def fromEavt(path: String): IvorySource =
    ???

  // FIX MTH up to here ............ depends on some other refactoring to get Versions, Repositories and Locations under control.
  def fromRepository(repository: Repository, store: FeatureStore): ResultT[IO, IvorySource] = repository match {
    case HdfsRepository(root, conf, run) =>
      Versions.readPrioritized(repository, store.factsets).map(versions => new IvorySource {
      def toDList: DList[(Priority, Fact)] =
        versions
          .groupBy(_._2).toList
          .map({ case (k, vs) => (k, vs.map(_._1)) })
          .map({ case (v, fss) =>
            (???): DList[(Priority, Factset, Fact)]
           }).reduce(_++_).map({ case (p, n, f) => (p, f) })
      })
    case LocalRepository(root) => ???
    case S3Repository(bucket, root, conf, client, tmp, run) => ???
  }

  def fromThriftExtract(path: String): IvorySource =
    ???
}
