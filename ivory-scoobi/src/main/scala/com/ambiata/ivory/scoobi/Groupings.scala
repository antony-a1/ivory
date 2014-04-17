package com.ambiata.ivory.scoobi

import scalaz.Ordering._
import com.nicta.scoobi.Scoobi._

object Groupings {
  type EntityId = String
  type AttributeName = String

  /**
   * This grouping will take a map of partitions to index and send each key to the reducer associated with the index.
   * If the key is not found it will use the String Grouping to determin which reducer to go to.
   * The index is mod'd with the total number of reducers so it will wrap if its greater.
   */
  def partitionGrouping(partitions: Map[String, Int]) = new Grouping[String] {
    val grp = implicitly[Grouping[String]]
    override def partition(k: String, total: Int): Int =
      partitions.get(k).map(i => i % total).getOrElse(grp.partition(k, total))

    override def groupCompare(x: String, y: String) =
      grp.groupCompare(x, y)
  }

  def sortGrouping = new Grouping[(EntityId, AttributeName)] {

    override def partition(key: (EntityId, AttributeName), howManyReducers: Int): Int = {
      implicitly[Grouping[EntityId]].partition(key._1, howManyReducers)
    }

    override def sortCompare(a: (EntityId, AttributeName), b: (EntityId, AttributeName)): scalaz.Ordering = {
      val entityIdOrdering = a._1.compareTo(b._1)

      entityIdOrdering match {
        case 0 => {
          fromInt(a._2.compareTo(b._2))
        }
        case x => fromInt(x)
      }
    }

    override def groupCompare(a: (EntityId, AttributeName), b: (EntityId, AttributeName)): scalaz.Ordering = {
      fromInt(a._1.compareTo(b._1))
    }
  }
}
