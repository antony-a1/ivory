package com.ambiata.ivory.storage.repository

import scalaz._, Scalaz._, effect.IO
import com.ambiata.saws.core._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.mundane.control._

/* This is a set of interfaces used to represent rank-n functions to force different computations to ResultT[IO, _]  */

trait ScoobiRun {
  def run[A](action: ScoobiAction[A]): ResultT[IO, A]
}

trait S3Run {
  def run[A](action: ScoobiS3Action[A]): ResultT[IO, A]
  def runS3[A](action: S3Action[A]): ResultT[IO, A]
  def runScoobi[A](action: S3Action[A]): ResultT[IO, A]
}
