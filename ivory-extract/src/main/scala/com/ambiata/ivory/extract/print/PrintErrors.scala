package com.ambiata.ivory.extract.print

import scalaz.concurrent.Task
import com.ambiata.ivory.core.ParseError
import com.ambiata.mundane.io.{IOActions, IOAction, Logger}
import scalaz.std.anyVal._
import com.ambiata.ivory.scoobi.SeqSchemas
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import IOActions._
import com.ambiata.ivory.alien.hdfs.Hdfs

/**
 * Read a ParseError sequence file and print it to screen
 */
object PrintErrors {

  def print(paths: List[Path], config: Configuration, delim: String): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- Print.printPathsWith(paths, config, SeqSchemas.parseErrorSeqSchema, printParseError(delim, l))
  } yield ()

  def printParseError(delim: String, logger: Logger)(path: Path, p: ParseError): Task[Unit] = Task.delay {
    val logged = Seq(p.line, p.message).mkString(delim)
    logger(logged).unsafePerformIO
  }
}
