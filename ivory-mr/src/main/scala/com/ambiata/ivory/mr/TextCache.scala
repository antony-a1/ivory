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
 * This is module for managing passing text data-types via tha distributed cache. This is
 * _unsafe_ at best, and should be used with extreme caution. The only valid reason to
 * use it is when writing raw map reduce jobs.
 */
case class TextCache(base: Path) {

  val distCache = DistCache(base)

  /* Push a text representation of a data-type to the distributed cache
     for this job, under the specified key. This fails _hard_ if anything
     goes wrong. */
  def push(job: Job, key: TextCache.Key, s: String): Unit =
    distCache.push(job, DistCache.Key(key.value), s.getBytes("UTF-8"))

  /* Pop a data-type from the distributed job using f, it is assumed that this is
     only run by map or reduce tasks where to the cache for this job where a call
     to TextCache#push has prepared everything. This fails _hard_ if anything
     goes wrong. */
  def pop[A](conf: Configuration, key: TextCache.Key, f: String => String \/ A): A =
    distCache.pop(conf, DistCache.Key(key.value), bytes => f(new String(bytes, "UTF-8")))
}

object TextCache {
  case class Key(value: String)
}
