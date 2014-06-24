package com.ambiata.ivory.extract.print

import scalaz.stream.{Sink, io}
import java.io.File
import scalaz.concurrent.Task
import scalaz.syntax.traverse._
import scalaz.std.list._
import org.apache.hadoop.io.{ByteWritable, SequenceFile, BytesWritable, NullWritable}
import org.apache.avro.mapred.SequenceFileReader
import scalaz.stream.Process
import scalaz.stream.Process.End
import com.ambiata.mundane.io.{IOActions, IOAction, Logger}
import scalaz.std.anyVal._
import com.ambiata.ivory.scoobi.{ScoobiAction, SeqSchemas}
import org.apache.hadoop.fs.{Path}
import org.apache.hadoop.conf.Configuration
import IOActions._
import com.ambiata.ivory.alien.hdfs.Hdfs
import com.ambiata.mundane.control.Result
import com.nicta.scoobi.io.sequence.SeqSchema

/**
 * Read a facts sequence file and print it to screen
 */
object Print {

  def printPathsWith[A](paths: List[Path], configuration: Configuration, schema: SeqSchema[A], printA: (Path, A) => Task[Unit]): IOAction[Unit] =
    paths.traverseU(path => for {
      files <- IOActions.fromResultT(Hdfs.globFiles(path, "*").filterHidden.run(configuration))
      _     <- files.traverse(file => printWith(file, configuration, schema, printA))
    } yield ()).void

  def printWith[A](path: Path, configuration: Configuration, schema: SeqSchema[A], printA: (Path, A) => Task[Unit]): IOAction[Unit] = IOActions.result { logger =>
    val reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(path))
    def readValue(r: SequenceFile.Reader): schema.SeqType = {
      val bytes = new BytesWritable()
      try {
        if (!r.next(NullWritable.get, bytes)) throw End
      } catch { case t: Throwable => throw End }
      bytes.asInstanceOf[schema.SeqType]
    }

    val console: Sink[Task, A] = io.channel(a => printA(path, a))

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
}
