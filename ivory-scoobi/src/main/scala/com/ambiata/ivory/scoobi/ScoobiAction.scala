package com.ambiata.ivory
package scoobi

import com.ambiata.mundane.control._
import scalaz.effect.IO
import scalaz._, Scalaz._
import \&/._
import com.nicta.scoobi.core.ScoobiConfiguration
import org.apache.hadoop.fs.{Path, FileSystem}
import alien.hdfs._

case class ScoobiAction[+A](action: ActionT[IO, Unit, ScoobiConfiguration, A]) {

  def run(conf: ScoobiConfiguration): ResultTIO[A] =
    action.executeT(conf)

  def map[B](f: A => B): ScoobiAction[B] =
    ScoobiAction(action.map(f))

  def flatMap[B](f: A => ScoobiAction[B]): ScoobiAction[B] =
    ScoobiAction(action.flatMap(a => f(a).action))

  def mapError(f: These[String, Throwable] => These[String, Throwable]): ScoobiAction[A] =
    ScoobiAction(action.mapError(f))

  def mapErrorString(f: String => String): ScoobiAction[A] =
    ScoobiAction(action.mapError(_.leftMap(f)))

  def |||[AA >: A](other: ScoobiAction[AA]): ScoobiAction[AA] =
    ScoobiAction(action ||| other.action)

  def flatten[B](implicit ev: A <:< ScoobiAction[B]): ScoobiAction[B] =
    flatMap(a => ev(a))

  def unless(condition: Boolean): ScoobiAction[Unit] =
    ScoobiAction.unless(condition)(this)
}


object ScoobiAction extends ActionTSupport[IO, Unit, ScoobiConfiguration] {

  def value[A](a: A): ScoobiAction[A] =
    ScoobiAction(super.ok(a))

  def ok[A](a: A): ScoobiAction[A] =
    value(a)

  def safe[A](a: => A): ScoobiAction[A] =
    ScoobiAction(super.safe(a))

  def fail[A](e: String): ScoobiAction[A] =
    ScoobiAction(super.fail(e))

  def fromDisjunction[A](d: String \/ A): ScoobiAction[A] = d match {
    case -\/(e) => fail(e)
    case \/-(a) => ok(a)
  }

  def fromValidation[A](v: Validation[String, A]): ScoobiAction[A] =
    fromDisjunction(v.disjunction)

  def fromIO[A](io: IO[A]): ScoobiAction[A] =
    ScoobiAction(super.fromIO(io))

  def fromResultTIO[A](res: ResultTIO[A]): ScoobiAction[A] =
    ScoobiAction(super.fromIOResult(res.run))

  def fromHdfs[A](action: Hdfs[A]): ScoobiAction[A] = for {
    sc <- ScoobiAction.scoobiConfiguration
    a  <- ScoobiAction.fromResultTIO(action.run(sc.configuration))
  } yield a

  def filesystem: ScoobiAction[FileSystem] =
    ScoobiAction(reader((sc: ScoobiConfiguration) => FileSystem.get(sc.configuration)))

  def scoobiConfiguration: ScoobiAction[ScoobiConfiguration] =
    ScoobiAction(reader(identity))

  def scoobiJob[A](f: ScoobiConfiguration => A): ScoobiAction[A] = for {
    sc <- ScoobiAction.scoobiConfiguration
    a  <- ScoobiAction.safe(f(sc))
  } yield a


  def unless[A](condition: Boolean)(action: ScoobiAction[A]): ScoobiAction[Unit] =
    if (!condition) action.map(_ => ()) else ScoobiAction.ok(())

  implicit def ScoobiActionMonad: Monad[ScoobiAction] = new Monad[ScoobiAction] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: ScoobiAction[A])(f: A => ScoobiAction[B]) = m.flatMap(f)
  }
}
