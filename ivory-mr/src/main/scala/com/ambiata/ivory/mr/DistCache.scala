package com.ambiata.ivory.mr

import com.ambiata.ivory.alien.hdfs._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import java.net.URI
import java.util.UUID

import scalaz._, Scalaz._, effect.IO

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job

/**
 * This is module for managing passing data-types via tha distributed cache. This is
 * _unsafe_ at best, and should be used with extreme caution. The only valid reason to
 * use it is when writing raw map reduce jobs.
 */
object DistCache {
  case class Key(value: String)

  /* Push a representation of a data-type to the distributed cache for this job, under the
     specified key. This fails _hard_ if anything goes wrong. Use DistCache#pop in the
     setup method of Mapper or Reducer to recover data. */
  def push(job: Job, key: Key, bytes: Array[Byte]): Unit = {
    val tmp = s"/tmp/${job.getJobName}-${UUID.randomUUID.toString}.dist-cache"
    val uri = new URI(tmp + "#" + key.value)
    (Hdfs.writeWith(new Path(tmp), Streams.writeBytes(_, bytes)) >> Hdfs.safe {
      addCacheFile(new URI(tmp + "#" + key.value), job)
    }).run(job.getConfiguration).run.unsafePerformIO match {
      case Ok(_) =>
        ()
      case Error(e) =>
        sys.error(s"Could not push $key to distributed cache: ${Result.asString(e)}")
    }
  }

  /* Pop a data-type from the distributed job using the specified function, it is
     assumed that this is only run by map or reduce tasks where to the cache for
     this job where a call to DistCache#push has prepared everything. This fails
     _hard_ if anything goes wrong. */
  def pop[A](key: Key, f: Array[Byte] => String \/ A): A =
    Files.readBytes(key.value.toFilePath).flatMap(bytes => ResultT.safe { f(bytes) }).run.unsafePerformIO match {
      case Ok(\/-(a)) =>
        a
      case Ok(-\/(s)) =>
        sys.error(s"Could not decode $key on pop from local path: ${s}")
      case Error(e) =>
        sys.error(s"Could not pop $key from local path: ${Result.asString(e)}")
    }

  def addCacheFile(uri: URI, job: Job) = {
    import com.nicta.scoobi.impl.util.Compatibility.cache
    cache.addCacheFile(uri, job.getConfiguration)
  }
}
