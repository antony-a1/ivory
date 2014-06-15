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
import org.apache.hadoop.mapreduce.Job

/**
 * This is module for managing passing thrift data-types via tha distributed case. This is
 * _unsafe_ at best, and should be used with extreme caution. The only valid reason to
 * use it is when writing raw map reduce jobs.
 */
object ThriftCache {
  case class Key(value: String)

  type ThriftLike = org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]

  val serializer = new TSerializer(new TCompactProtocol.Factory)
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  /* Push a thrift data-type to the distributed cache for this job, under the
     specified key. This fails _hard_ if anything goes wrong. */
  def push[A](job: Job, key: Key, a: A)(implicit ev: A <:< ThriftLike): Unit = {
    val tmp = s"/tmp/${job.getJobName}-${UUID.randomUUID.toString}.thrift"
    val uri = new URI(tmp + "#" + key.value)
    val action = toHdfs(new Path(tmp), a) >> Hdfs.safe {
      job.addCacheFile(new URI(tmp + "#" + key.value))
    }
    action.run(job.getConfiguration).run.unsafePerformIO match {
      case Ok(_) =>
        ()
      case Error(e) =>
        sys.error(s"Could not push $key to distributed cache: ${Result.asString(e)}")
    }
  }

  /* Pop a thrift data-type from the distributed job, it is assumed that this is
     only run by map or reduce tasks where to the cache for this job where a call
     to ThriftCache#push has prepared everything. This fails _hard_ if anything
     goes wrong. */
  def pop[A](key: Key, a: A)(implicit ev: A <:< ThriftLike): Unit =
    fromLocal(key.value.toFilePath, a).run.unsafePerformIO match {
      case Ok(_) =>
        ()
      case Error(e) =>
        sys.error(s"Could not pop $key from local path: ${Result.asString(e)}")
    }

  def fromLocal[A](path: FilePath, a: A)(implicit ev: A <:< ThriftLike): ResultT[IO, Unit] =
    Files.readBytes(path).flatMap(bytes => ResultT.safe { deserializer.deserialize(a, bytes) })

  def toHdfs[A](path: Path, a: A)(implicit ev: A <:< ThriftLike): Hdfs[Unit]=
    Hdfs.writeWith(path, Streams.writeBytes(_, serializer.serialize(a)))
}
