package com.ambiata.ivory.alien.hdfs

import org.specs2.Specification
import org.apache.hadoop.fs.Path
import java.io.File
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.apache.hadoop.conf.Configuration

class HdfsSpec extends Specification { def is = s2"""

 The Hdfs object provide functions to deal with paths
   it is possible to recursively glob paths                              $e1
   can create a new dir, changing the name when it already exists        $e2
   run out of names when trying to change a dir                          $e3
"""

  val basedir = "target/test/HdfsSpec/" + java.util.UUID.randomUUID()

  def e1 = {
    val dirs = Seq(basedir + "/e1/a/b/c", basedir + "/e1/e/f/g")
    val files = dirs.flatMap(dir => Seq(dir+"/f1", dir+"/f2"))
    dirs.foreach(dir => new File(dir).mkdirs)
    files.foreach(f => new File(f).createNewFile)

    Hdfs.globFilesRecursively(new Path(basedir)).run(new Configuration) must beOkLike(paths => paths must haveSize(4))
  }

  def e2 = {
    val dir = new Path(basedir + "/e2/a/b/c")

    val newDir = for {
      _ <- Hdfs.mkdir(dir)
      d <- Hdfs.mkdirWithRetry(dir, name => Some(name + "1"))
    } yield d

    newDir.run(new Configuration) must beOkLike(path => path must beSome(new Path(dir.getParent, dir.getName + "1")))
  }

  def e3 = {
    val dir = new Path(basedir + "/e2/a/b/c")

    val newDir = for {
      _ <- Hdfs.mkdir(dir)
      d <- Hdfs.mkdirWithRetry(dir, name => None)
    } yield d

    newDir.run(new Configuration) must beOkLike(path => path must beNone)
  }
}
