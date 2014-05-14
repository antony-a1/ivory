package com.ambiata.ivory.core

import scalaz._, Scalaz._

/* Seconds since start of day */
class Time private(val underlying: Int) extends AnyVal {
  def seconds =
    underlying

  override def toString: String =
    seconds.toString

  def hhmmss = {
    val (hh, resth) = (seconds / 3600, seconds % 3600)
    val (mm, restm) = (resth / 60, resth % 60)
    val ss = restm / 60
    hh+":"+mm+":"+ss
  }
}

object Time {
  def apply(seconds: Int): Time =
    macro Macros.literal

  def unsafe(seconds: Int): Time =
    new Time(seconds)

  def isValid(seconds: Int): Boolean =
    seconds >= 0 && seconds < (60 * 60 * 24)

  def create(seconds: Int): Option[Time] =
    isValid(seconds).option(unsafe(seconds))

  object Macros {
    import scala.reflect.macros.Context
    import language.experimental.macros

    def literal(c: Context)(seconds: c.Expr[Int]): c.Expr[Time] = {
      import c.universe._
      seconds match {
        case Expr(Literal(Constant(s: Int))) =>
          create(s) match {
            case None =>
              c.abort(c.enclosingPosition, s"This is not a valid time literal Time($s).")
            case Some(date) =>
              c.Expr(q"com.ambiata.ivory.core.Time.unsafe($s)")
          }
        case _ =>
          c.abort(c.enclosingPosition, s"Not a literal ${showRaw(seconds)}")
      }
    }
  }
}
