package com.ambiata.ivory.mr

import org.apache.hadoop.fs.Path

import com.ambiata.ivory.alien.hdfs.Hdfs

import scalaz._, Scalaz._

/**
 * Move tmp output to final locations on hdfs
 */
object Committer {

  def commitInto(context: MrContext, target: Path, cleanup: Boolean): Hdfs[Unit] =
    commitWith(context, source => new Path(target, source.getName), cleanup)

  def commitWith(context: MrContext, mapping: Path => Path, cleanup: Boolean): Hdfs[Unit] = for {
    paths <- Hdfs.globPaths(context.output, "*")
    _     <- paths.traverse(p => { val t = mapping(p); Hdfs.mkdir(t.getParent) >> Hdfs.mv(p, t) })
    _     <- if(cleanup) context.cleanup else Hdfs.ok(())
  } yield ()
}
