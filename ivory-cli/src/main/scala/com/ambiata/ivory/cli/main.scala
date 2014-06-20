package com.ambiata.ivory.cli

import com.nicta.scoobi.core._
import com.nicta.scoobi.Scoobi._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import org.apache.hadoop.conf.Configuration
import scalaz._, Scalaz._
import scalaz.effect._

object main {

  val commands: List[IvoryApp] = List(
    catErrors,
    catFacts,
    catRepositoryFacts,
    chord,
    countFacts,
    createFeatureStore,
    createRepository,
    factDiff,
    generateDictionary,
    generateFacts,
    importDictionary,
    importFacts,
    importFeatureStore,
    ingest,
    ingestBulk,
    pivot,
    pivotSnapshot,
    recompress,
    snapshot,
    validateFactSet,
    validateStore
  )

  def main(args: Array[String]): Unit = {
    val program = for {
      (progName, argsRest) <- args.headOption.map(_ -> args.tail)
      command <- commands.find(_.cmd.parser.programName == progName)
    } yield command.cmd.run(argsRest)
    // End of the universe
    program.sequence.flatMap(_.flatten.fold(usage())(_ => IO.ioUnit)).unsafePerformIO
  }

  def usage() = IO {
    val cmdNames = commands.map(_.cmd.parser.programName).mkString("|")
    println(s"Usage: {$cmdNames}")
  }
}

case class IvoryCmd[A](parser: scopt.OptionParser[A], initial: A, runner: IvoryRunner[A]) {

  def run(args: Array[String]): IO[Option[Unit]] = {
    runner match {
      case ActionCmd(f) => parseAndRun(args, f andThen (_.executeT(consoleLogging).map(_ => Nil)))
      case HadoopCmd(f) => parseAndRun(args, f(new Configuration()))
      // TODO Simply usage of ScoobiApp where possible
      // https://github.com/ambiata/ivory/issues/27
      case ScoobiCmd(f) => IO {
        // Hack to avoid having to copy logic from ScoobiApp
        // To make this harder we need ScoobiApp to remove its own args before we begin
        // May Tony have mercy on my soul
        var scoobieResult: Option[Unit] = None
        new ScoobiApp {
          // Need to evaluate immediately to avoid the main() method of ScoobiApp cleaning up too soon
          def run = scoobieResult = parseAndRun(args, f(configuration)).unsafePerformIO
        }.main(args)
        scoobieResult
      }
    }
  }

  private def parseAndRun(args: Seq[String], result: A => ResultTIO[List[String]]): IO[Option[Unit]] = {
    parser.parse(args, initial)
      .map(result andThen {
        _.run.map(_.fold(_.foreach(println), e => { println(s"Failed! - ${Result.asString(e)}"); sys.exit(1) }))
      }).sequence
  }
}

/**
 * Represents the different types of runners in an Ivory program,
 * so that any required setup can be handled in a single place
 */
sealed trait IvoryRunner[A]
case class ActionCmd[A](f: A => IOAction[Unit]) extends IvoryRunner[A]
case class HadoopCmd[A](f: Configuration => A => ResultTIO[List[String]]) extends IvoryRunner[A]
case class ScoobiCmd[A](f: ScoobiConfiguration => A => ResultTIO[List[String]]) extends IvoryRunner[A]

trait IvoryApp {
  def cmd: IvoryCmd[_]

  // Only here for direct execution of apps for backwards compatibility
  def main(args: Array[String]): Unit = {
    cmd.run(args).unsafePerformIO
  }
}
