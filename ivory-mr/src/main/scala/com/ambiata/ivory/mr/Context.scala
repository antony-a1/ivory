package com.ambiata.ivory.mr

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import java.util.UUID

import com.ambiata.ivory.alien.hdfs._

/**
 * This is used to handle tmp paths for output and dist cache
 */
case class MrContext(id: String) {
  val tmpBase: Path =
    new Path(s"/tmp/${id}")

  val output: Path =
    new Path(tmpBase, "output")

  val distCache: DistCache =
    DistCache(new Path(tmpBase, "dist-cache"))

  val thriftCache: ThriftCache =
    ThriftCache(new Path(tmpBase, "dist-cache-thrift"))

  val textCache: TextCache =
    TextCache(new Path(tmpBase, "dist-cache-text"))

  def cleanup: Hdfs[Unit] =
    Hdfs.deleteAll(tmpBase)
}

object MrContext {
  /**
   * Create a new MrContext from the namespace and job. This will mutate
   * the job configuration to set the context id so it can be re-created
   * again from the configuration object.
   */
  def newContext(namespace: String, job: Job): MrContext = {
    val id = s"${namespace}-${job.getJobName}-${UUID.randomUUID.toString}"
    job.getConfiguration.set(MrContext.Keys.Id, id)
    MrContext(id)
  }

  def fromConfiguration(conf: Configuration): MrContext =
    MrContext(conf.get(MrContext.Keys.Id))

  object Keys {
    val Id = "ivory.ctx.id"
  }
}
