package com.ambiata.ivory.storage.repository

import scalaz._, Scalaz._, effect.IO
import com.ambiata.saws.core._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.mundane.control._
import com.nicta.scoobi.core.ScoobiConfiguration

/* This is a set of interfaces used to represent rank-n functions to force different computations to ResultT[IO, _]  */

trait ScoobiRun {
  def runScoobi[A](action: ScoobiAction[A]): ResultT[IO, A]
}

trait S3Run extends ScoobiRun {
  def runScoobiS3[A](action: ScoobiS3Action[A]): ResultT[IO, A]
  def runS3[A](action: S3Action[A]): ResultT[IO, A]
}

object ScoobiRun {
  def apply(c: ScoobiConfiguration): ScoobiRun = new ScoobiRun {
    def runScoobi[A](action: ScoobiAction[A]) = action.run(c)
  }
}

object S3Run {
  def apply(c: ScoobiConfiguration): S3Run = new S3Run {
    def runScoobi[A](action: ScoobiAction[A]) = action.run(c)
    def runScoobiS3[A](action: ScoobiS3Action[A]) = action.runScoobi(c).evalT
    def runS3[A](action: S3Action[A]): ResultT[IO, A] = action.evalT

  }
}
