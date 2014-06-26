package com.ambiata.ivory.storage.repository

import com.ambiata.mundane.io.{FilePath, Logger}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.nicta.scoobi.Scoobi._
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.alien.hdfs._

import scalaz.{DList => _, _}, Scalaz._, \&/._
import scalaz.effect.IO
import StatAction._

case class RecreateAction[+A](action: ActionT[IO, Unit, RecreateConfig, A]) {
  def run(conf: RecreateConfig): ResultTIO[A] = 
    validate.flatMap(_ => this).action.executeT(conf)
  
  def validate: RecreateAction[Unit] = RecreateAction.configuration flatMap { c =>
    RecreateAction.fail(s"Repository '${c.hdfsFrom}' is the same as '${c.hdfsTo}'")
      .when(c.hdfsFrom.root == c.hdfsTo.root) >>
    RecreateAction.fail(s"Repository '${c.hdfsTo.root}' already exists")
      .when(!c.overwrite)
      .when(RecreateAction.fromHdfs(Hdfs.exists(c.hdfsTo.root.toHdfs)))
  }

  def map[B](f: A => B): RecreateAction[B] =
    RecreateAction(action.map(f))

  def flatMap[B](f: A => RecreateAction[B]): RecreateAction[B] =
    RecreateAction(action.flatMap(a => f(a).action))

  def mapError(f: These[String, Throwable] => These[String, Throwable]): RecreateAction[A] =
    RecreateAction(action.mapError(f))

  def mapErrorString(f: String => String): RecreateAction[A] =
    RecreateAction(action.mapError(_.leftMap(f)))

  def |||[AA >: A](other: RecreateAction[AA]): RecreateAction[AA] =
    RecreateAction(action ||| other.action)

  def flatten[B](implicit ev: A <:< RecreateAction[B]): RecreateAction[B] =
    flatMap(a => ev(a))

  def log(f: A => String): RecreateAction[Unit] =
    flatMap((a: A) => RecreateAction.log(f(a)))

  def logged(f: A => String): RecreateAction[A] =
    flatMap((a: A) => RecreateAction.log(f(a)).flatMap(_ => RecreateAction.safe(a)))

  def log(message: =>String): RecreateAction[Unit] =
    flatMap((a: A) => RecreateAction.log(message))

  def logged(message: =>String): RecreateAction[A] =
    flatMap((a: A) => RecreateAction.log(message).flatMap(_ => RecreateAction.safe(a)))

  def unless(condition: Boolean): RecreateAction[Unit] =
    RecreateAction.unless(condition)(this)

  def unless(condition: RecreateAction[Boolean]): RecreateAction[Unit] =
    condition.flatMap(unless)

  def when(condition: Boolean): RecreateAction[Unit] =
    RecreateAction.when(condition)(this)

  def when(condition: RecreateAction[Boolean]): RecreateAction[Unit] =
    condition.flatMap(when)

}

object RecreateAction extends ActionTSupport[IO, Unit, RecreateConfig] {

  def log(message: String) =
    configuration.flatMap((config: RecreateConfig) => fromIO(config.logger(message)))

  def configuration: RecreateAction[RecreateConfig] =
    RecreateAction(reader(identity))

  def value[A](a: A): RecreateAction[A] =
    RecreateAction(super.ok(a))

  def ok[A](a: A): RecreateAction[A] =
    value(a)

  def safe[A](a: => A): RecreateAction[A] =
    RecreateAction(super.safe(a))

  def fromIO[A](a: =>IO[A]): RecreateAction[A] =
    RecreateAction(super.fromIO(a))

  def fail[A](e: String): RecreateAction[A] =
    RecreateAction(super.fail(e))

  def fromScoobi[A](action: ScoobiAction[A]): RecreateAction[A] = for {
    c <- configuration
    a <- fromResultTIO(action.run(c.sc))
  } yield a

  def fromHdfs[A](action: Hdfs[A]): RecreateAction[A] =
    fromScoobi(ScoobiAction.fromHdfs(action))

  def fromResultTIO[A](res: ResultTIO[A]): RecreateAction[A] =
    RecreateAction(super.fromIOResult(res.run))

  def fromStat[A](repo: Repository, action: StatAction[A]): RecreateAction[A] = for {
    c <- configuration
    a <- fromResultTIO(action.run(StatConfig(c.sc.configuration, repo)))
  } yield a

  def unless[A](condition: Boolean)(action: RecreateAction[A]): RecreateAction[Unit] =
    when(!condition)(action)

  def when[A](condition: Boolean)(action: RecreateAction[A]): RecreateAction[Unit] =
    if (condition) action.void else RecreateAction.ok(())

  implicit def RecreateActionMonad: Monad[RecreateAction] = new Monad[RecreateAction] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: RecreateAction[A])(f: A => RecreateAction[B]) = m.flatMap(f)
  }
}

case class RecreateConfig(from: Repository, to: Repository,
                          sc: ScoobiConfiguration, codec: Option[CompressionCodec],
                          reduce: Boolean, clean: Boolean, dry: Boolean, recreateData: List[RecreateData],
                          overwrite: Boolean,
                          logger: Logger) {
  val (hdfsFrom, hdfsTo) = (from, to) match {
    case (f: HdfsRepository, t: HdfsRepository) => (f, t)
    case _ => sys.error(s"Repository combination '$from' and '$to' not supported!")
  }

  def dryFor(data: RecreateData) =
    dry || !recreateData.contains(data)
}

class RecreateData
object RecreateData {
  val DICTIONARY = new RecreateData
  val STORE      = new RecreateData
  val FACTSET    = new RecreateData
  val SNAPSHOT   = new RecreateData
  val ALL        = List(DICTIONARY, STORE, FACTSET, SNAPSHOT)

  def parse(s: String) = {
    def parseElement(string: String) =
      if (string == "dictionary")    Some(DICTIONARY)
      else if (string == "store")    Some(STORE)
      else if (string == "factset")  Some(FACTSET)
      else if (string == "snapshot") Some(SNAPSHOT)
      else                           None

    val result = s.toLowerCase.split(",").map(_.trim).map(parseElement).flatten.distinct.toList
    if (result.nonEmpty) result
    else                 ALL
  }
}
