package com.ambiata.ivory.data

import scalaz._, Scalaz._

sealed trait Ref
case class IdentifierRef(identifier: Identifier) extends Ref
case class TagRef(tag: Key) extends Ref
case object Head extends Ref
