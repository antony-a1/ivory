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
import com.ambiata.ivory.core.Fact
import com.ambiata.mundane.io.{IOActions, IOAction, Logger}
import scalaz.std.anyVal._
import com.ambiata.ivory.scoobi.{ScoobiAction, SeqSchemas}
import org.apache.hadoop.fs.{Path}
import org.apache.hadoop.conf.Configuration
import IOActions._
import com.ambiata.ivory.alien.hdfs.Hdfs

/**
 * Read a facts sequence file and print it to screen
 */
object PrintFacts {

  def printGlob(path: String, glob: String, delim: String, tombstone: String): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- Print.printGlobWith(path, glob, SeqSchemas.factSeqSchema, printFact(delim, tombstone, l))
  } yield ()

  def printPaths(paths: List[Path], config: Configuration, delim: String, tombstone: String): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- Print.printPathsWith(paths, config, SeqSchemas.factSeqSchema, printFact(delim, tombstone, l))
  } yield ()

  def print(path: Path, config: Configuration, delim: String, tombstone: String): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- Print.printWith(path, config, SeqSchemas.factSeqSchema, printFact(delim, tombstone, l))
  } yield ()

  def printFact(delim: String, tombstone: String, logger: Logger)(path: Path, f: Fact): Task[Unit] = Task.delay {
    val logged =
      Seq(f.entity,
          f.namespace,
          f.feature,
          if(f.isTombstone) tombstone else f.value.stringValue.getOrElse(""),
          f.date.hyphenated+delim+f.time.hhmmss).mkString(delim)

    logger(logged).unsafePerformIO
  }
}
