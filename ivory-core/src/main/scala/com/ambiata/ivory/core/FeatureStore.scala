package com.ambiata.ivory.core

import scalaz._, Scalaz._, \&/._
import scalaz.effect._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._

/** The feature store is simply an ordered list of path references to fact sets. */
case class FeatureStore(factsets: List[PrioritizedFactset]) {
  def +++(other: FeatureStore): FeatureStore =
    FeatureStore(PrioritizedFactset.concat(factsets, other.factsets))

  def ---(other: FeatureStore): FeatureStore =
    FeatureStore(PrioritizedFactset.diff(factsets, other.factsets))

  /** @return a FeatureStore having only the sets accepted by the predicate */
  def filter(predicate: String => Boolean) =
    FeatureStore(factsets.filter(set => predicate(set.set.name)))

  /** @return a FeatureStore that contains everything in this minus what is in other */
  def diff(other: FeatureStore) =
    FeatureStore(PrioritizedFactset.diff(this.factsets, other.factsets))
}
