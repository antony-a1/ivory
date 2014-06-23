package com.ambiata.ivory.mr

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import java.util.UUID

import com.ambiata.ivory.alien.hdfs._

/**
 * This is used to handle tmp paths for output and dist cache
 */
sealed trait MrContext {
  val id: String

  lazy val tmpBase: Path =
    new Path(s"/tmp/${id}")

  lazy val output: Path =
    new Path(tmpBase, "output")

  lazy val distCache: DistCache =
    DistCache(new Path(tmpBase, "dist-cache"))

  lazy val thriftCache: ThriftCache =
    ThriftCache(new Path(tmpBase, "dist-cache-thrift"))

  lazy val textCache: TextCache =
    TextCache(new Path(tmpBase, "dist-cache-text"))

  def cleanup: Hdfs[Unit] =
    Hdfs.deleteAll(tmpBase)
}

object MrContext {
  def newContext(namespace: String, job: Job): MrContext = new MrContext {
    val id: String =
      s"${namespace}-${job.getJobName}-${UUID.randomUUID.toString}"

    job.getConfiguration.set(MrContext.Keys.Id, id)
  }

  def fromConfiguration(conf: Configuration): MrContext = new MrContext {
    val id: String =
      conf.get(MrContext.Keys.Id)
  }

  object Keys {
    val Id = "ivory.ctx.id"
  }
}
