package com.ambiata.ivory.core

import org.specs2._, matcher._, specification._
import java.io.File
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scalaz._, Scalaz._

class PartitionSpec extends Specification { def is = s2"""

Partition Tests
----------

  Glob pattern for partitions works       $glob

"""

  lazy val conf = new Configuration
  lazy val filesystem = FileSystem.get(conf)

  val basedir = "target/test/PartitionSpec/" + java.util.UUID.randomUUID()

  def glob = {
    val base = basedir + "/compress"
    val partitions = List(Partition("fs1", "ns1", Date(2012, 1, 1), Some(base)),
                          Partition("fs1", "ns1", Date(2012, 2, 1), Some(base)),
                          Partition("fs1", "ns1", Date(2013, 1, 1), Some(base)),
                          Partition("fs1", "ns2", Date(2012, 1, 1), Some(base)),
                          Partition("fs2", "ns2", Date(2012, 1, 1), Some(base)))

    partitions.foreach(p => {
      new File(p.path).mkdirs
      new File(p.path + "/f1").createNewFile
      new File(p.path + "/f2").createNewFile
    })

    val actual = Partitions.globPath(partitions) 
    filesystem.globStatus(new Path(actual)).toList.map(_.getPath.toString) must haveSize(10)
  }
}

