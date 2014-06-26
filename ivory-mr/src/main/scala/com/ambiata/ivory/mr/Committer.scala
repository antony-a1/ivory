package com.ambiata.ivory.mr

import org.apache.hadoop.fs.Path

import com.ambiata.ivory.alien.hdfs.Hdfs

import scalaz._, Scalaz._

/**
 * Move tmp output to final locations on hdfs
 */
object Committer {

  /**
   * Move the children of each dir under context.output to the directory specified by the mapping
   * function.
   *
   * The mapping function takes the name of the dir under context.output and must return
   * a destination directory. If the destination dir doesn't exist, it will be created.
   * If the destination dir or any of the children in context.output is a file, an error
   * will be returned.
   */
  def commit(context: MrContext, mapping: String => Path, cleanup: Boolean): Hdfs[Unit] = for {
    paths <- Hdfs.globPaths(context.output, "*").filterHidden
    _     <- paths.traverse(p => for {
               t <- Hdfs.value(mapping(p.getName))
               d <- Hdfs.isDirectory(p)
               _ <- if(!d) Hdfs.fail(s"Can not commit '${p}' as its not a dir") else Hdfs.ok(())
               _ <- Hdfs.mkdir(t)
               s <- Hdfs.globPaths(p, "*")
               _ <- s.traverse(subpath => Hdfs.mv(subpath, t))
             } yield ())
    _     <- if(cleanup) context.cleanup else Hdfs.ok(())
  } yield ()
}
