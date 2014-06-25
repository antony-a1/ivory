package com.ambiata.ivory.mr

import org.specs2._
import org.scalacheck._, Arbitrary._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.io._

import com.ambiata.ivory.alien.hdfs._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import java.util.UUID

class CommitterSpec extends Specification with ScalaCheck { def is = s2"""

Committer
-----------

  Can move files into single target        $e1
  Can move path into differnt targets      $e2

"""
  def e1 = {
    val c = new Configuration
    val ctx = MrContext(UUID.randomUUID.toString)
    val target = new Path("target/test/CommitterSpec-" + UUID.randomUUID.toString)

    writeFile(new Path(ctx.output, "f1"), "test1").run(c) must beOk
    Committer.commitInto(ctx, target, true).run(c) must beOk
    readFile(new Path(target, "f1")).run(c) must beOkLike(_ must_== "test1")
  }

  def e2 = {
    val c = new Configuration
    val ctx = MrContext(UUID.randomUUID.toString)
    val target = new Path("target/test/CommitterSpec-" + UUID.randomUUID.toString)

    (for {
      _ <- writeFile(new Path(ctx.output, "path1/f1"), "test1")
      _ <- writeFile(new Path(ctx.output, "path2/f2"), "test2")
    } yield ()).run(c) must beOk

    Committer.commitWith(ctx, p => { if(p == "path1") new Path(target, "p1") else new Path(target, "p2") }, true).run(c) must beOk

    readFile(new Path(target, "p1/f1")).run(c) must beOkLike(_ must_== "test1")
    readFile(new Path(target, "p2/f2")).run(c) must beOkLike(_ must_== "test2")
  }

  def readFile(path: Path): Hdfs[String] =
    Hdfs.readWith(path, is => Streams.read(is))

  def writeFile(path: Path, content: String): Hdfs[Unit] =
    Hdfs.writeWith(path, os => Streams.write(os, content))
}
