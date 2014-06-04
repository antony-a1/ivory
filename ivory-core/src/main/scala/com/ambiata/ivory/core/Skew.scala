package com.ambiata.ivory.core


/*
 * Calculation to work out optimal groupings.
 */
object Skew {
  /* given a list of namespaces and their data size, calculate the reduce buckets for each namespace/feature */
  def calculate(dict: Dictionary, n: List[(String, Long)], optimal: Long): (Int, List[(String, String, Int)]) =
    n.foldLeft(0 -> List[(String, String, Int)]())({ case ((allocated, acc), (namespace, size)) =>
      val features = dict.forNamespace(namespace).meta.keys.map(_.name).toList
      val count = features.size
      val potential = (size / optimal).toInt + 1
      val x = features.zipWithIndex.map({
        case (feature, idx) => (namespace, feature, allocated + (idx % potential))
      })
      (allocated + math.min(potential, count) , x ::: acc)
    })
}
