package com.ambiata.ivory.data

import scalaz._, Scalaz._

case class Key private (render: String) {
  override def toString =
    render
}

object Key {
  def create(s: String): Option[Key] =
    (!s.isEmpty && s.forall(c => c.isLetterOrDigit || c == '-' || c == '_' || c == '.')).option(Key(s))
}
