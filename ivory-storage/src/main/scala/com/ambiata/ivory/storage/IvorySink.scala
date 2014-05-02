package com.ambiata.ivory.storage

import com.ambiata.ivory.core._
import com.nicta.scoobi._, Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

sealed trait IvorySink {
  def toDList(facts: DList[Fact]): DList[Fact]
  def toSink: Sink[Task, Fact]
}

object IvorySink {
  def toEavt(path: String): IvorySink =
    ???

  def toRepository(path: Repository, factset: String): IvorySink =
    ???

  def toThriftExtract(path: Repository, factset: String): IvorySink =
    ???

  def toTextAsDenseRow(path: Repository, factset: String): IvorySink =
    ???
}
