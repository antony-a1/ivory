package com.ambiata.ivory.extract.print

import java.io.File
import scalaz.concurrent.Task
import scalaz.syntax.traverse._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.{-\/, \/-}
import scalaz.stream.{Sink, io, Process}
import scalaz.stream.Process.End
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import com.ambiata.mundane.io.{IOActions, IOAction, Logger}
import IOActions._

import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.storage.legacy.PartitionFactThriftStorageV2
import com.ambiata.ivory.scoobi.{ScoobiAction, SeqSchemas}
import com.ambiata.ivory.alien.hdfs.Hdfs

/**
 * Read a facts sequence file and print it to screen
 */
object PrintInternalFacts {

  def printGlob(path: String, glob: String, delim: String, tombstone: String): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- Print.printGlobWith(path, glob, SeqSchemas.thriftFactSeqSchema, printFact(delim, tombstone, l))
  } yield ()

  def printPaths(paths: List[Path], config: Configuration, delim: String, tombstone: String): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- Print.printPathsWith(paths, config, SeqSchemas.thriftFactSeqSchema, printFact(delim, tombstone, l))
  } yield ()

  def print(path: Path, config: Configuration, delim: String, tombstone: String): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- Print.printWith(path, config, SeqSchemas.thriftFactSeqSchema, printFact(delim, tombstone, l))
  } yield ()

  def printFact(delim: String, tombstone: String, logger: Logger)(path: Path, f: ThriftFact): Task[Unit] = Task.delay {
    val logged = PartitionFactThriftStorageV2.parseFact(path.toString, f) match {
      case -\/(perr) => s"Error - ${perr.message}, line ${perr.line}"
      case \/-(f)    =>
        Seq(f.entity,
            f.namespace,
            f.feature,
            if(f.isTombstone) tombstone else f.value.stringValue.getOrElse(""),
            f.date.hyphenated+delim+f.time.hhmmss).mkString(delim)
    }
    logger(logged).unsafePerformIO
  }
}
