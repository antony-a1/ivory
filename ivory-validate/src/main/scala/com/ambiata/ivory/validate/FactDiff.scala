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

  def partitionFacts(input1: String, input2: String, outputPath: String): ScoobiAction[Unit] = for {
    res <- ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
             val dlist1 = PartitionFactThriftStorageV1.PartitionedFactThriftLoader(List(input1)).loadScoobi.map({
               case -\/(e) => sys.error(s"Can not parse fact - ${e}")
               case \/-(f) => f
             })
             val dlist2 = PartitionFactThriftStorageV1.PartitionedFactThriftLoader(List(input2)).loadScoobi.map({
               case -\/(e) => sys.error(s"Can not parse fact - ${e}")
               case \/-(f) => f
             })
             (dlist1, dlist2)
           })
    (dlist1, dlist2) = res
    _   <- scoobiJob(dlist1, dlist2, outputPath)
  } yield ()

  def flatFacts(input1: String, input2: String, outputPath: String): ScoobiAction[Unit] = for {
    res <- ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
             val dlist1 = valueFromSequenceFile[Fact](input1)
             val dlist2 = valueFromSequenceFile[Fact](input2)
             (dlist1, dlist2)
           })
    (dlist1, dlist2) = res
    _   <- scoobiJob(dlist1, dlist2, outputPath)
  } yield ()

  def scoobiJob(first_facts: DList[Fact], second_facts: DList[Fact], outputPath: String): ScoobiAction[Unit] = {
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>

      val facts = first_facts.map((true, _)) ++ second_facts.map((false, _))

      val grp = facts.groupBy({ case (flag, fact) => (fact.entity, fact.featureId.toString, fact.date.int, fact.time.seconds, fact.value.stringValue) })

      val diff: DList[List[(Boolean, Fact)]] = grp.mapFlatten({ case (_, vs) =>
        vs.toList match {
          case (true, f1) :: (false, f2) :: Nil => None
          case (false, f2) :: (true, f1) :: Nil => None
          case other                            => Some(other)
        }
      })

      val out: DList[String] = diff.map({
        case (true, fact) :: Nil  => s"Fact '${fact}' does not exist in input2"
        case (false, fact) :: Nil => s"Fact '${fact}' does not exist in input1"
        case g                    => s"Found duplicates - '${g}'"
      })

      persist(out.toTextFile(outputPath, overwrite = true))
    })
  }
}
