package com.ambiata.ivory.scoobi

import com.nicta.scoobi.Scoobi._

object Groupings {

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
}
