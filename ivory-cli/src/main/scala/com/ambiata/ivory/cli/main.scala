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
    createFeatureStore,
    createRepository,
    factDiff,
    generateDictionary,
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
      result <- command.cmd.run(argsRest)
    } yield result
    // End of the universe
    program.getOrElse(usage()).unsafePerformIO
  }

  def usage() = IO {
    val cmdNames = commands.map(_.cmd.parser.programName).mkString("|")
    println(s"Usage: {$cmdNames}")
  }
}

case class IvoryCmd[A](parser: scopt.OptionParser[A], initial: A, runner: IvoryRunner[A]) {

  def run(args: Array[String]): Option[IO[Unit]] = {
    val result = runner match {
      case ActionCmd(f) => f andThen (_.executeT(consoleLogging).map(_ => ""))
      case HadoopCmd(f) => f(new Configuration())
      // TODO Do we need to use ScoobiApp? Is there a nice(r) way of doing that?
      // https://github.com/ambiata/ivory/issues/27
      case ScoobiCmd(f) => f(ScoobiConfiguration())
    }
    parser.parse(args, initial)
      .map(result andThen {
        _.run.map(_.fold(println, e => { println(s"Failed! - ${Result.asString(e)}"); sys.exit(1) }))
      })
  }
}

/**
 * Represents the different types of runners in an Ivory program,
 * so that any required setup can be handled in a single place
 */
sealed trait IvoryRunner[A] 
case class ActionCmd[A](f: A => IOAction[Unit]) extends IvoryRunner[A]
case class HadoopCmd[A](f: Configuration => A => ResultTIO[String]) extends IvoryRunner[A]
case class ScoobiCmd[A](f: ScoobiConfiguration => A => ResultTIO[String]) extends IvoryRunner[A]

trait IvoryApp {
  def cmd: IvoryCmd[_]

  // Only here for direct execution of apps for backwards compatibility
  def main(args: Array[String]): Unit = {
    cmd.run(args).foreach(_.unsafePerformIO)
  }
}
