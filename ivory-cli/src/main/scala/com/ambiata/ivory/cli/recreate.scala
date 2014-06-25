package com.ambiata.ivory.cli

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.io._

import com.ambiata.ivory.storage.repository._

object recreate extends ScoobiApp {
  case class CliArguments(input: String, output: String, reduce: Boolean, clean: Boolean, dry: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("ivory-recreate") {
    head("""Clone an ivory repository, recompressing each part file and storing in a the latest format.""")

    help("help") text "shows this usage text"

    opt[String]('i', "input")  action { (x, c) => c.copy(input = x) }     required() text "Input ivory repository."

    opt[String]('o', "output") action { (x, c) => c.copy(output = x) }    required() text "Output ivory repository."

    opt[Unit]('d', "dry-run")  action { (_, c) => c.copy(dry = true) }    optional() text "Do a dry run only."

    opt[Unit]('r', "reduce")   action { (_, c) => c.copy(reduce = true) } optional() text "Run reducer to combine small files."

    opt[Unit]("no-clean")      action { (_, c) => c.copy(clean = false) } optional() text "Do not clean out empty factsets from stores."

  }

  def run {
    parser.parse(args, CliArguments(input = "", output = "", reduce = false, clean = true, dry = false)).map { c =>
      val rconf = RecreateConfig(from = Repository.fromHdfsPath(FilePath(c.input), configuration),
                                 to = Repository.fromHdfsPath(FilePath(c.output), configuration),
                                 sc = configuration,
                                 codec = Some(new SnappyCodec),
                                 reduce = c.reduce,
                                 clean = c.clean,
                                 dry = c.dry,
                                 logger = consoleLogging)
      Recreate.all.run(rconf).run.unsafePerformIO.fold(
        ok =>  { println("Done!"); ok },
        err => { println(err); sys.exit(1) }
      )
    }
  }
}
