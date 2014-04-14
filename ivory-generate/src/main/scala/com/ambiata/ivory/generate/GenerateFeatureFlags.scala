package com.ambiata.ivory.generate

import scalaz._, Scalaz._, \&/._, effect._
import com.nicta.rng._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse._

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._

case class FeatureFlags(namespace: String, name: String, sparcity: Double, frequency: Frequency) {

  def toDelimitedString(delim: Char): String = {
    val fields = List(s"$namespace", s"$name", s"$sparcity", s"$frequency")
    fields.mkString(delim.toString)
  }
}

object FeatureFlags {

  def fromInputStream(is: java.io.InputStream): ResultTIO[List[FeatureFlags]] = for {
    content <- Streams.read(is)
    r <- ResultT.fromDisjunction[IO, List[FeatureFlags]](fromLines(content.lines.toList).leftMap(This(_)))
  } yield r

  def fromHdfs(path: Path): Hdfs[List[FeatureFlags]] =
    Hdfs.readWith(path, fromInputStream)

  def fromLines(lines: List[String]): String \/ List[FeatureFlags] = {
    val numbered = lines.zipWithIndex.map({ case (l, n) => (l, n + 1) })
    numbered.map({ case (l, n) => parseEntry(l).leftMap(e => s"Line $n: $e")}).sequenceU
  }

  def parseEntry(line: String): String \/ FeatureFlags = {
    import ListParser._
    val parser: ListParser[FeatureFlags] = for {
      namespace <- string
      name      <- string
      sparcity  <- double
      frequency <- for {
        s <- string
        p <- getPosition
        r <- value(Frequency.parse(s).leftMap(_ => s"""Not a valid frequency at position $p: '$s'"""))
      } yield r
    } yield FeatureFlags(namespace, name, sparcity, frequency)
    parser.run(Delimited.parsePsv(line)).disjunction
  }
}

sealed trait Frequency
case class Daily() extends Frequency {
  override def toString = "daily"
}
case class Weekly() extends Frequency {
  override def toString = "weekly"
}
case class Monthly() extends Frequency {
  override def toString = "monthly"
}

object Frequency {
  def parse(s: String): Validation[String, Frequency] =
    s match {
      case "daily"   => Daily().success
      case "weekly"  => Weekly().success
      case "monthly" => Monthly().success
      case otherwise => "".failure
    }
}

object GenerateFeatureFlags {
  import Rng._

  type Namespace = String
  type Name = String
  type Sparcity = Double

  lazy val r = new java.util.Random

  def onHdfs(dict: Dictionary, output: Path): Hdfs[Unit] = for {
    flags <- Hdfs.fromIO(randomFlags(dict).run)
    _     <- Hdfs.writeWith(output, os => Streams.write(os, flags.map(_.toDelimitedString('|')).mkString("\n")))
  } yield ()

  def randomFlags(dict: Dictionary): Rng[List[FeatureFlags]] =
    dict.meta.toList.traverse({ case (FeatureId(ns, name), _) => randomFlagEntry(ns, name) })

  def randomFlagEntry(namespace: Namespace, name: Name): Rng[FeatureFlags] = for {
    f <- randomFrequency
    s <- randomSparcity
  } yield FeatureFlags(namespace, name, s, f)

  def randomFrequency: Rng[Frequency] =
    // choose monthly more often, then weekly, then daily
    frequency((1, insert(Daily())), (5, insert(Weekly())), (10, insert(Monthly())))

  // TODO this should probably be changed to a Gaussian distribution
  def randomSparcity: Rng[Double] =
    choosedouble(0.0, 1.0)
}
