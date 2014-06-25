package com.ambiata.ivory.mr

import org.apache.hadoop.fs.Path

import com.ambiata.ivory.alien.hdfs.Hdfs

import scalaz._, Scalaz._

/**
 * Move tmp output to final locations on hdfs
 */
object Committer {

  /**
   * Move each file/dir under context.output into the given target dir.
   */
  def commitInto(context: MrContext, target: Path, cleanup: Boolean): Hdfs[Unit] =
    commitWith(context, source => new Path(target, source), cleanup)

  /**
   * Move each file/dir under context.output to the path specified by the mapping function.
   *
   * The mapping function takes the name of the file/dir under context.output and must return
   * the qualified target path (not a parent directory).
   *
   * This function will fail if any of the target paths produced by the mapping function
   * already exists.
   */
  def commitWith(context: MrContext, mapping: String => Path, cleanup: Boolean): Hdfs[Unit] = for {
    paths <- Hdfs.globPaths(context.output, "*")
    _     <- paths.traverse(p => for {
               t <- Hdfs.value(mapping(p.getName))
               e <- Hdfs.exists(t)
               _ <- if(e) Hdfs.fail(s"Target ${t} already existis!") else Hdfs.ok(())
               _ <- Hdfs.mkdir(t.getParent)
               _ <- Hdfs.mv(p, t)
             } yield ())
    _     <- if(cleanup) context.cleanup else Hdfs.ok(())
  } yield ()
}
