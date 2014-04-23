package com.ambiata.ivory.data

import scalaz._, Scalaz._

class Identifier private (val n: Int) extends AnyVal {
  def render: String =
    String.format("%08x", java.lang.Integer.valueOf(n))

  override def toString: String =
    render

  def next: Option[Identifier] =
    (n != 0xffffffff).option(new Identifier(n + 1))

  def order(i: Identifier): Ordering =
    n ?|? i.n
}

object Identifier {
  def initial: Identifier =
    new Identifier(0)

  def parse(s: String): Option[Identifier] = try {
    val l = java.lang.Long.parseLong(s, 16)
    if (l > 0xffffffffL) None else Some(new Identifier(l.toInt))
  } catch {
    case e: NumberFormatException => None
  }

  implicit def IdentifierOrder: Order[Identifier] =
    Order.order(_ order _)

  implicit def IdentifierOrdering =
    IdentifierOrder.toScalaOrdering
}
