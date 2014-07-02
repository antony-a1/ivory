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
 * This is module for managing passing data-types via the distributed cache. This is
 * _unsafe_ at best, and should be used with extreme caution. The only valid reason to
 * use it is when writing raw map reduce jobs.
 */
case class DistCache(base: Path, contextId: ContextId) {

  /* Push a representation of a data-type to the distributed cache for this job, under the
     specified key. A namespace is added to the key to make it unique for each instance
     of DistCache and is maintained through the configuration object. This fails _hard_ if
     anything goes wrong. Use DistCache#pop in the setup method of Mapper or Reducer to
     recover data. */
  def push(job: Job, key: DistCache.Key, bytes: Array[Byte]): Unit = {
    val nskey = key.namespaced(contextId.value)
    val tmp = s"${base}/${nskey.combined}"
    val uri = new URI(tmp + "#" + nskey.combined)
    (Hdfs.writeWith(new Path(tmp), Streams.writeBytes(_, bytes)) >> Hdfs.safe {
      addCacheFile(new URI(tmp + "#" + nskey.combined), job)
    }).run(job.getConfiguration).run.unsafePerformIO match {
      case Ok(_) =>
        ()
      case Error(e) =>
        sys.error(s"Could not push $nskey to distributed cache: ${Result.asString(e)}")
    }
  }

  /* Pop a data-type from the distributed job using the specified function, it is
     assumed that this is only run by map or reduce tasks where to the cache for
     this job where a call to DistCache#push has prepared everything. This fails
     _hard_ if anything goes wrong. */
  def pop[A](conf: Configuration, key: DistCache.Key, f: Array[Byte] => String \/ A): A = {
    val nskey = key.namespaced(contextId.value)
    Files.readBytes(findCacheFile(conf, nskey).toFilePath).flatMap(bytes => ResultT.safe { f(bytes) }).run.unsafePerformIO match {
      case Ok(\/-(a)) =>
        a
      case Ok(-\/(s)) =>
        sys.error(s"Could not decode ${nskey} on pop from local path: ${s}")
      case Error(e) =>
        sys.error(s"Could not pop ${nskey} from local path: ${Result.asString(e)}")
    }
  }
  def addCacheFile(uri: URI, job: Job): Unit = {
    import com.nicta.scoobi.impl.util.Compatibility.cache
    cache.addCacheFile(uri, job.getConfiguration)
  }

  def findCacheFile(conf: Configuration, nskey: DistCache.NamespacedKey): String =
    if (org.apache.hadoop.util.VersionInfo.getVersion.contains("cdh4"))
      com.nicta.scoobi.impl.util.Compatibility.cache.getLocalCacheFiles(conf).toList.find(_.getName == nskey.combined).getOrElse(sys.error("Could not find $nskey to pop from local path.")).toString
    else
      nskey.combined
}

object DistCache {
  case class Key(value: String) {
    def namespaced(ns: String): NamespacedKey =
      NamespacedKey(ns, value)
  }
  case class NamespacedKey(namespace: String, value: String) {
    def combined: String =
      s"${namespace}${value}"
  }

  object Keys {
    val namespace = "ivory.dist-cache.namespace"
  }
}
