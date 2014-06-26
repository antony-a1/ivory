package com.ambiata.ivory.mr

import com.ambiata.ivory.alien.hdfs._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import java.net.URI
import java.util.UUID

import scalaz._, Scalaz._, effect.IO

import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.{TSerializer, TDeserializer}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

/**
 * This is module for managing passing thrift data-types via tha distributed cache. This is
 * _unsafe_ at best, and should be used with extreme caution. The only valid reason to
 * use it is when writing raw map reduce jobs.
 */
case class ThriftCache(base: Path, id: ContextId) {
  val distCache = DistCache(base, id)
  val serializer = new TSerializer(new TCompactProtocol.Factory)
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  type ThriftLike = org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]

  /* Push a thrift data-type to the distributed cache for this job, under the
     specified key. This fails _hard_ if anything goes wrong. */
  def push[A](job: Job, key: ThriftCache.Key, a: A)(implicit ev: A <:< ThriftLike): Unit =
    distCache.push(job, DistCache.Key(key.value), serializer.serialize(a))

  /* Pop a thrift data-type from the distributed job, it is assumed that this is
     only run by map or reduce tasks where to the cache for this job where a call
     to ThriftCache#push has prepared everything. This fails _hard_ if anything
     goes wrong. NOTE: argument is updated, rather than a new value returned. */
  def pop[A](conf: Configuration, key: ThriftCache.Key, a: A)(implicit ev: A <:< ThriftLike): Unit =
    distCache.pop(conf, DistCache.Key(key.value), bytes => \/.fromTryCatch(deserializer.deserialize(a, bytes)).leftMap(_.toString))
}

object ThriftCache {
  case class Key(value: String)
}
