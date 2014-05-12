package com.ambiata.ivory
package scoobi

import com.ambiata.mundane.control.{ActionTSupport, ResultT, Result, ActionT}
import scalaz.effect.IO
import org.apache.hadoop.conf.Configuration
import com.ambiata.saws.core._
import com.nicta.scoobi.core.ScoobiConfiguration
import alien.hdfs._
import scalaz._, Scalaz._
import com.amazonaws.services.s3.AmazonS3Client

case class ScoobiAwsAction[A, R](action: ActionT[IO, Vector[AwsLog], (ScoobiConfiguration, R), A]) {

  def runScoobi(sc: ScoobiConfiguration): Aws[R, A] =
    Aws(ActionT((r: R) => action.runT((sc, r))))

  def runScoobiLog(sc: ScoobiConfiguration)(implicit client: Client[R]): IO[(Vector[AwsLog], Result[A])] =
    runScoobi(sc).evalWithLog(client)

  def runScoobiAws(sc: ScoobiConfiguration)(implicit client: Client[R]): IO[Result[A]] =
    runScoobi(sc).eval(client)

  def runScoobiAwsT(sc: ScoobiConfiguration)(implicit client: Client[R]): ResultT[IO, A] =
    runScoobi(sc).evalT(client)

}

object ScoobiAwsAction {
  implicit def ScoobiAwsActionMonad[R]: Monad[({type l[a]=ScoobiAwsAction[a, R]})#l] = new Monad[({type l[a]=ScoobiAwsAction[a, R]})#l] {
    type hf[A] = ScoobiAwsAction[A, R]

    def point[A](a: => A) = ScoobiAwsAction(ActionT.safe(a))

    def bind[A, B](fa: hf[A])(f: A => hf[B]) =
      ScoobiAwsAction(fa.action.flatMap(a => f(a).action))
  }
}

object ScoobiS3Action extends ActionTSupport[IO, Vector[AwsLog], (ScoobiConfiguration, AmazonS3Client)] { outer =>

  type Config = (ScoobiConfiguration, AmazonS3Client)

  def value[A](a: A) =
    ScoobiAwsAction(super.ok(a))

  def ok[A](a: A) =
    value(a)

  def safe[A](a: =>A) =
    ScoobiAwsAction(super.safe(a))

  def fail[A](e: String) =
    ScoobiAwsAction(super.fail(e))

  def scoobiConfiguration: ScoobiAwsAction[ScoobiConfiguration, AmazonS3Client] =
    fromScoobiAction(ScoobiAction.scoobiConfiguration)

  def reader[A](f: ScoobiConfiguration => A): ScoobiAwsAction[A, AmazonS3Client] =
    scoobiConfiguration.map(f)

  def configuration: ScoobiAwsAction[Configuration, AmazonS3Client] =
  fromHdfs(Hdfs.configuration)

  def fromScoobiAction[A](action: ScoobiAction[A]): ScoobiAwsAction[A, AmazonS3Client] =
    ScoobiAwsAction(ActionT((c: Config) => ActionT.fromResultT[IO, Vector[AwsLog], Config, A](action.run(c._1)).runT(c)))

  def fromResultTIO[A](action: ResultT[IO, A]): ScoobiS3Action[A] =
    fromScoobiAction(ScoobiAction.fromResultTIO(action))

  def fromS3Action[A](action: S3Action[A]): ScoobiAwsAction[A, AmazonS3Client] =
    ScoobiAwsAction(action.runT.contramap((_: Config)._2))

  def fromHdfs[A](hdfs: Hdfs[A]): ScoobiAwsAction[A, AmazonS3Client] =
    fromHdfsS3(HdfsS3Action.fromHdfs(hdfs))

  def fromHdfsS3[A](hdfs: HdfsS3Action[A]): ScoobiAwsAction[A, AmazonS3Client] =
    ScoobiAwsAction(ActionT((c: Config) => ActionT.fromIOResult[IO, Vector[AwsLog], Config, A](hdfs.action.run((c._1.configuration, c._2)).map(_._2)).runT(c)))

  implicit def ScoobiS3ActionMonad: Monad[ScoobiS3Action] = ScoobiAwsAction.ScoobiAwsActionMonad[AmazonS3Client]

}

object ScoobiS3EMRAction extends ActionTSupport[IO, Vector[AwsLog], (ScoobiConfiguration, AmazonS3EMRClient)] { outer =>

  type Config = (ScoobiConfiguration, AmazonS3EMRClient)

  def value[A](a: A) =
    ScoobiAwsAction(super.ok(a))

  def safe[A](a: =>A) =
    ScoobiAwsAction(super.safe(a))

  def scoobiConfiguration: ScoobiAwsAction[ScoobiConfiguration, AmazonS3EMRClient] =
    fromScoobiAction(ScoobiAction.scoobiConfiguration)

  def reader[A](f: ScoobiConfiguration => A): ScoobiS3EMRAction[A] =
    scoobiConfiguration.map(f)

  def configuration: ScoobiAwsAction[Configuration, AmazonS3EMRClient] =
    fromHdfs(Hdfs.configuration)

  def fromScoobiAction[A](action: ScoobiAction[A]): ScoobiS3EMRAction[A] =
    ScoobiAwsAction(ActionT((c: Config) => ActionT.fromResultT[IO, Vector[AwsLog], Config, A](action.run(c._1)).runT(c)))

  def fromResultTIO[A](action: ResultT[IO, A]): ScoobiS3EMRAction[A] =
    fromScoobiAction(ScoobiAction.fromResultTIO(action))

  def fromS3Action[A](action: S3Action[A]): ScoobiS3EMRAction[A] =
    ScoobiAwsAction(action.runT.contramap((_: Config)._2._1))

  def fromEMRAction[A](action: EMRAction[A]): ScoobiS3EMRAction[A] =
    ScoobiAwsAction(action.runT.contramap((_: Config)._2._2))

  def fromHdfs[A](hdfs: Hdfs[A]): ScoobiS3EMRAction[A] =
    fromHdfsS3(HdfsS3Action.fromHdfs(hdfs))

  def fromHdfsS3[A](hdfs: HdfsS3Action[A]): ScoobiS3EMRAction[A] =
    ScoobiAwsAction(ActionT((c: Config) => ActionT.fromIOResult[IO, Vector[AwsLog], Config, A](hdfs.action.run((c._1.configuration, c._2._1)).map(_._2)).runT(c)))

  implicit def ScoobiS3EMRActionMonad: Monad[ScoobiS3EMRAction] = ScoobiAwsAction.ScoobiAwsActionMonad[AmazonS3EMRClient]

}
