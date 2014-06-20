package com.ambiata.ivory.extract

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._
import WireFormats._
import FactFormats._

object FactCount {

  def flatFacts(path: Path): ScoobiAction[Long] = ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
    val facts = valueFromSequenceFile[Fact](path.toString)
    facts.size.run
  })

}
