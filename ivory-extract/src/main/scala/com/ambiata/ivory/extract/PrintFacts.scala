package com.ambiata.ivory.extract

import scalaz.stream.{Sink, io}
import java.io.File
import scalaz.concurrent.Task
import scalaz.syntax.traverse._
import scalaz.std.list._
import org.apache.hadoop.io.{ByteWritable, SequenceFile, BytesWritable, NullWritable}
import org.apache.avro.mapred.SequenceFileReader
import scalaz.stream.Process
import scalaz.stream.Process.End
import com.ambiata.ivory.core.Fact
import com.ambiata.mundane.io.{IOActions, IOAction, Logger}
import scalaz.std.anyVal._
import com.ambiata.ivory.scoobi.{ScoobiAction, SeqSchemas}
import org.apache.hadoop.fs.{Path}
import org.apache.hadoop.conf.Configuration
import IOActions._
import com.ambiata.ivory.alien.hdfs.Hdfs
import com.ambiata.mundane.control.Result

/**
 * Read a facts sequence file and print it to screen
 */
object PrintFacts {

  def print(path: String, glob: String, delimiter: String, tombstone: String): IOAction[Unit] = {
    val configuration = new Configuration
    for {
      paths <- IOActions.fromResultT(Hdfs.globFilesRecursively(new Path(path), glob).run(configuration))
      _     <- print(paths, configuration, delimiter, tombstone)
    } yield ()
  }

  def print(paths: List[Path], configuration: Configuration, delimiter: String, tombstone: String): IOAction[Unit] =
    paths.map(path => print(path, configuration, delimiter, tombstone)).sequenceU.map(_ => ())

  def print(path: Path, configuration: Configuration, delimiter: String, tombstone: String): IOAction[Unit] = IOActions.result { logger =>
    val schema = SeqSchemas.factSeqSchema
    val reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(path))
    def readValue(r: SequenceFile.Reader): schema.SeqType = {
      val bytes = new BytesWritable()
      try {
        if (!r.next(NullWritable.get, bytes)) throw End
      } catch { case t: Throwable => throw End }
      bytes.asInstanceOf[schema.SeqType]
    }

    val console: Sink[Task, Fact] = io.channel(printFact(delimiter, tombstone, logger))

    val source: Process[Task, schema.SeqType] =
      io.resource(Task.delay(reader))(r => Task.delay(r.close))(
        r => Task.delay(readValue(r)))

    val read =
      source
          .map(schema.fromWritable)
          .to(console)

    read.run.attemptRun.fold(
      e => Result.fail(e.getMessage),
      u => Result.ok(u)
    )
  }

  def printFact(delimiter: String, tombstone: String, logger: Logger)(f: Fact): Task[Unit] = Task.delay {
    val logged =
      if (f.isTombstone) tombstone
      else               Seq(f.namespace, f.entity, f.value.stringValue.getOrElse(""), f.date.hyphenated+delimiter+f.time.hhmmss).mkString(delimiter)

    logger(logged).unsafePerformIO
  }
}
