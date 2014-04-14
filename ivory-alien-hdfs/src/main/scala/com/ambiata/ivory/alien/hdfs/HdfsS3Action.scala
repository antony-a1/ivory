package com.ambiata.ivory.alien.hdfs

import com.ambiata.saws.core._
import scalaz.Monad
import com.ambiata.mundane.control._
import scalaz._, Scalaz._
import \&/._
import org.apache.hadoop.conf.Configuration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import scalaz.effect.IO
import org.apache.hadoop.fs.FileSystem

case class HdfsAwsAction[A, R](action: ActionT[IO, Vector[AwsLog], (Configuration, R), A]) {

  def runHdfs(configuration: Configuration): Aws[R, A] =
    Aws(ActionT((r: R) => action.runT((configuration, r))))

  def runHdfsLog(configuration: Configuration)(implicit client: Client[R]): IO[(Vector[AwsLog], Result[A])] =
    runHdfs(configuration).evalWithLog(client)

  def runHdfsAws(configuration: Configuration)(implicit client: Client[R]): IO[Result[A]] =
    runHdfs(configuration).eval(client)

  def runHdfsAwsT(configuration: Configuration)(implicit client: Client[R]): ResultT[IO, A] =
    runHdfs(configuration).evalT(client)

}

object HdfsS3Action extends ActionTSupport[IO, Vector[AwsLog], (Configuration, AmazonS3Client)] { outer =>

  type Config = (Configuration, AmazonS3Client)

  def value[A](a: A) =
    HdfsAwsAction(super.ok(a))

  def safe[A](a: =>A) =
    HdfsAwsAction(super.safe(a))

  def configuration: HdfsS3Action[Configuration] =
    fromHdfs(Hdfs.configuration)

  def fromAction[A](action: S3Action[A]): HdfsAwsAction[A, AmazonS3Client] =
    HdfsAwsAction(action.runT.contramap((_: Config)._2))

  def fromHdfs[A](hdfs: Hdfs[A]): HdfsAwsAction[A, AmazonS3Client] =
    HdfsAwsAction(ActionT((c: Config) => ActionT.fromIOResult[IO, Vector[AwsLog], Config, A](hdfs.action.run(c._1).map(_._2)).runT(c)))

  def fromResultT[A](result: ResultT[IO, A]): HdfsS3Action[A] =
    HdfsAwsAction(ActionT.fromIOResult[IO, Vector[AwsLog], Config, A](result.run))

  implicit def HdfsS3ActionMonad: Monad[HdfsS3Action] = HdfsAwsActionMonad[AmazonS3Client]

  implicit def HdfsAwsActionMonad[R]: Monad[({type l[a]=HdfsAwsAction[a, R]})#l] = new Monad[({type l[a]=HdfsAwsAction[a, R]})#l] {
    type hf[A] = HdfsAwsAction[A, R]

    def point[A](a: => A) = HdfsAwsAction(ActionT.safe(a))

    def bind[A, B](fa: hf[A])(f: A => hf[B]) =
      HdfsAwsAction(fa.action.flatMap(a => f(a).action))
  }
}

