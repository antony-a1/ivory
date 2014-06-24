package com.ambiata.ivory.mr

import com.ambiata.ivory.alien.hdfs._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import java.net.URI
import java.util.UUID

import scalaz._, Scalaz._, effect.IO

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

/**
 * This is module for managing passing data-types via tha distributed cache. This is
 * _unsafe_ at best, and should be used with extreme caution. The only valid reason to
 * use it is when writing raw map reduce jobs.
 */
case class DistCache(base: Path) {

  /* Push a representation of a data-type to the distributed cache for this job, under the
     specified key. This fails _hard_ if anything goes wrong. Use DistCache#pop in the
     setup method of Mapper or Reducer to recover data. */
  def push(job: Job, key: DistCache.Key, bytes: Array[Byte]): Unit = {
    val tmp = s"${base}/${key.value}"
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
  def pop[A](conf: Configuration, key: DistCache.Key, f: Array[Byte] => String \/ A): A = {
    Files.readBytes(findCacheFile(conf, key).toFilePath).flatMap(bytes => ResultT.safe { f(bytes) }).run.unsafePerformIO match {
      case Ok(\/-(a)) =>
        a
      case Ok(-\/(s)) =>
        sys.error(s"Could not decode $key on pop from local path: ${s}")
      case Error(e) =>
        sys.error(s"Could not pop $key from local path: ${Result.asString(e)}")
    }
  }
  def addCacheFile(uri: URI, job: Job): Unit = {
    import com.nicta.scoobi.impl.util.Compatibility.cache
    cache.addCacheFile(uri, job.getConfiguration)
  }

  def findCacheFile(conf: Configuration, key: DistCache.Key): String =
    if (org.apache.hadoop.util.VersionInfo.getVersion.contains("cdh4"))
      com.nicta.scoobi.impl.util.Compatibility.cache.getLocalCacheFiles(conf).toList.find(_.getName == key.value).getOrElse(sys.error("Could not find $key to pop from local path.")).toString
    else
      key.value
}

object DistCache {
  case class Key(value: String)
}
