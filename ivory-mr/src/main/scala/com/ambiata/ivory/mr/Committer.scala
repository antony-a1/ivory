package com.ambiata.ivory.mr

import org.apache.hadoop.fs.Path

import com.ambiata.ivory.alien.hdfs.Hdfs

import scalaz._, Scalaz._

/**
 * Move tmp output to final locations on hdfs
 */
object Committer {

  def commitSingle(context: MrContext, target: Path, cleanup: Boolean): Hdfs[Unit] = for {
    _ <- Hdfs.mv(context.output, target)
    _ <- if(cleanup) context.cleanup else Hdfs.ok(())
  } yield ()

  def commitMulti(context: MrContext, target: Path => Path, cleanup: Boolean): Hdfs[Unit] = for {
    paths <- Hdfs.globPaths(context.output, "*")
    _     <- paths.traverse(p => Hdfs.mv(p, target(p)))
    _     <- if(cleanup) context.cleanup else Hdfs.ok(())
  } yield ()
}
