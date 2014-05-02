package com.ambiata.ivory.validate

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.scoobi._
import WireFormats._
import FactFormats._

object FactDiff {

  def scoobiJob(input1: String, input2: String, outputPath: String, errorPath: String): ScoobiAction[Unit] = {
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      val (first_errs, first_facts) = byflag(PartitionFactThriftStorageV1.PartitionedFactThriftLoader(input1).loadScoobi, true)
      val (second_errs, second_facts) = byflag(PartitionFactThriftStorageV1.PartitionedFactThriftLoader(input2).loadScoobi, false)

      val errors = first_errs ++ second_errs
      val facts = first_facts ++ second_facts

      val grp = facts.groupBy({ case (flag, fact) => (fact.entity, fact.featureId.toString, fact.date.int, fact.time.seconds, fact.value.stringValue) })

      val diff: DList[List[(Boolean, Fact)]] = grp.mapFlatten({ case (_, vs) =>
        vs.toList match {
          case (true, f1) :: (false, f2) :: Nil => None
          case (false, f2) :: (true, f1) :: Nil => None
          case other                            => Some(other)
        }
      })

      val out: DList[String] = diff.map({
        case (true, fact) :: Nil  => s"Fact '${fact}' does not exist in ${input2}"
        case (false, fact) :: Nil => s"Fact '${fact}' does not exist in ${input1}"
        case g                    => s"Found duplicates - '${g}'"
      })

      val error_out: DList[String] = errors.map({
        case (true, e)  => s"${e} - ${input1}"
        case (false, e) => s"${e} - ${input2}"
      })

      persist(error_out.toTextFile(errorPath), out.toTextFile(outputPath))
    })
  }

  def byflag(dlist: DList[String \/ Fact], flag: Boolean): (DList[(Boolean, String)], DList[(Boolean, Fact)]) = {
    val errs = dlist.collect {
      case -\/(e) => (flag, e)
    }

    val facts = dlist.collect {
      case \/-(f) => (flag, f)
    }
    (errs, facts)
  }
}
