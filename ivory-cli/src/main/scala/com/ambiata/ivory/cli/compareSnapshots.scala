package com.ambiata.ivory.cli

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.lib.Relational
import scalaz.{DList => _, Value => _, _}, Scalaz._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.api._, Ivory._, IvoryRetire._

import scala.collection.JavaConverters._

object compareSnapshots extends IvoryApp {

  case class CliArguments(snap1: String, snap2: String, output: String)

  val parser = new scopt.OptionParser[CliArguments]("compare-snapshots") {
    head("""
         |App to compare two snapshots
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]("snap1")  action { (x, c) => c.copy(snap1 = x) }  required() text s"Hdfs path to the first snapshot."
    opt[String]("snap2")  action { (x, c) => c.copy(snap2 = x) }  required() text s"Hdfs path to the second snapshot."
    opt[String]("output") action { (x, c) => c.copy(output = x) } required() text s"Hdfs path to store results."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", ""), ScoobiCmd {
    configuration => c =>
      val banner = s"""======================= CompareSnapshot =======================
                      |
                      |Arguments --
                      |
                      |  Snap1                    : ${c.snap1}
                      |  Snap2                    : ${c.snap2}
                      |  Output                   : ${c.output}
                      |
                      |""".stripMargin
      println(banner)

      (for {
        snap1 <- snapshotFromHdfs(new Path(c.snap1))
        snap2 <- snapshotFromHdfs(new Path(c.snap2))
        _     <- ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
                   val dlist1: DList[(String, String)] = snap1.map(byKey("snap1"))
                   val dlist2: DList[(String, String)] = snap2.map(byKey("snap2"))

                   val diff = Relational(dlist1).joinFullOuter(dlist2, missing("snap2"), missing("snap1"), compare).mapFlatten({
                     case (_, err) => err
                   })

                   diff.toTextFile(c.output).persist
                 })
      } yield ()).run(configuration).map(_ => List(banner, s"Output path: $c.output", "Status -- SUCCESS"))
  })

  def compare(eat: String, v1: String, v2: String): Option[String] =
    if(v1 != v2) Some(s"'${eat}' has value '${v1}' in snapshot1, but '${v2}' in snapshot2") else None

  def missing(name: String)(eavt: String, v: String): Option[String] =
    Some(eavt + " does not exist in " + name)

  def byKey(name: String)(f: ParseError \/ Fact): (String, String) = f match {
    case -\/(e) => sys.error(s"Can not parse one of that facts in ${name}")
    case \/-(f) => (s"${f.entity}|${f.namespace}|${f.feature}|${f.datetime.localIso8601}", f.value.toString)
  }
}

