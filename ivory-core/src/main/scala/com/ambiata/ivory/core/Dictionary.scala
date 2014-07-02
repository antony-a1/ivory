package com.ambiata.ivory.core

import scalaz._, Scalaz._
import scala.math.{Ordering => SOrdering}

/** The feature dictionary is simply a look up of metadata for a given identifier/name. */
case class Dictionary(meta: Map[FeatureId, FeatureMeta]) {

  /** Create a `Dictionary` from `this` only containing features in the specified namespace. */
  def forNamespace(namespace: String): Dictionary =
    Dictionary(meta filter { case (fid, _) => fid.namespace === namespace })

  /** Create a `Dictionary` from `this` only containing the specified features. */
  def forFeatureIds(featureIds: Set[FeatureId]): Dictionary =
    Dictionary(meta filter { case (fid, _) => featureIds.contains(fid) })

  /** append the mappings coming from another dictionary */
  def append(other: Dictionary) =
    Dictionary(meta ++ other.meta)
}

case class FeatureId(namespace: String, name: String) {
  override def toString =
    toString(":")

  def toString(delim: String): String =
    s"${namespace}${delim}${name}"
}

object FeatureId {
  implicit val orderingByNamespace: SOrdering[FeatureId] =
    SOrdering.by(f => (f.namespace, f.name))
}

case class FeatureMeta(encoding: Encoding, ty: Type, desc: String, tombstoneValue: List[String] = List("☠")) {
}

sealed trait Encoding
case object BooleanEncoding   extends Encoding
case object IntEncoding       extends Encoding
case object LongEncoding      extends Encoding
case object DoubleEncoding    extends Encoding
case object StringEncoding    extends Encoding

sealed trait Type
case object NumericalType   extends Type
case object ContinuousType  extends Type
case object CategoricalType extends Type
case object BinaryType      extends Type
