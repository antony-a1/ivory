package com.ambiata.ivory.ingest

import com.ambiata.mundane.control._
import com.ambiata.mundane.io.FilePath

import com.ambiata.ivory.core._
import com.ambiata.ivory.metadata.Versions
import com.ambiata.ivory.storage._, PartitionFactThriftStorageV1._, IvoryStorage._, EavtTextStorageV1._
import com.ambiata.ivory.thrift._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.alien.hdfs._

import com.nicta.scoobi.Scoobi._

import java.io.{FileSystem => _, _}

import org.joda.time.DateTimeZone
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import scalaz.{DList => _, _}, Scalaz._, effect.IO

// FIX move to com.ambiata.ivory.ingest.internal
// FIX this is a prototype only, don't rely on anything until it has been verified
object HdfsDirectEavtTextImporter {
  type Channels = scala.collection.mutable.Map[String, SequenceFile.Writer]

  def direct(conf: Configuration, repository: HdfsRepository, dictionary: Dictionary, factset: String, namespace: String,
             path: Path, errorPath: Path, timezone: DateTimeZone, codec: Option[CompressionCodec],
             preprocess: String => String): ResultT[IO, Unit] = ResultT.safe[IO, Unit] {

    val serializer = ThriftSerialiser()
    val fs = FileSystem.get(conf)
    val err = new BufferedWriter(new OutputStreamWriter(fs.create(errorPath)))
    val channels: Channels = scala.collection.mutable.Map.empty
    val in = fs.open(path)
    val reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(in, 128 * 1024 * 1024), "UTF-8"))
    var line: String = null
    try {

      while ({line = reader.readLine; line != null }) {

        EavtTextStorageV1.parseFact(dictionary, namespace, timezone, preprocess).run(EavtTextStorageV1.splitLine(line)) match {
          case Success(fact) => {
            val path = Partition.path(namespace, fact.date)

            val writer = channels.getOrElseUpdate(path, {
              val opts = List(
                SequenceFile.Writer.file(new Path(new Path(repository.factsetPath(factset), path), "facts")),
                SequenceFile.Writer.keyClass(classOf[NullWritable]),
                SequenceFile.Writer.valueClass(classOf[BytesWritable])
              ) ++ (codec.map(c => SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, c)).toList)
              SequenceFile.createWriter(conf, opts:_*)
            })

            writer.append(NullWritable.get, new BytesWritable(serializer.toBytes(fact.asThrift)))
          }
          case Failure(e) =>
            err.write(e); err.newLine
        }

      }
    } finally {
      err.close
      channels.values.foreach(_.close)
      in.close
    }
  }
}
