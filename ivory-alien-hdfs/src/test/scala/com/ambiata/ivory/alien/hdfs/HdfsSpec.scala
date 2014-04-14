package com.ambiata.ivory.alien.hdfs

import org.specs2.Specification
import org.apache.hadoop.fs.Path
import java.io.File
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.apache.hadoop.conf.Configuration

class HdfsSpec extends Specification { def is = s2"""

 The Hdfs object provide functions to deal with paths
   it is possible to recursively glob paths  $e1

"""

 val basedir = "target/test/HdfsSpec/" + java.util.UUID.randomUUID()

 def e1 = {
   val dirs = Seq(basedir + "/a/b/c", basedir + "/e/f/g")
   val files = dirs.flatMap(dir => Seq(dir+"/f1", dir+"/f2"))
   dirs.foreach(dir => new File(dir).mkdirs)
   files.foreach(f => new File(f).createNewFile)

   Hdfs.globFilesRecursively(new Path(basedir)).run(new Configuration) must beOkLike(paths => paths must haveSize(4))
 }

}
