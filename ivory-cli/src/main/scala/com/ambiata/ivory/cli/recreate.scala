package com.ambiata.ivory.cli

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.io._

import com.ambiata.ivory.storage.repository._

object recreate extends ScoobiApp {
  case class CliArguments(input: String, output: String, dry: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("ivory-recreate") {
    head("""Clone an ivory repository, recompressing each part file and storing in a the latest format.""")

    help("help") text "shows this usage text"

    opt[String]('i', "input")        action { (x, c) => c.copy(input = x) } required() text "Input ivory repository."

    opt[String]('o', "output")       action { (x, c) => c.copy(output = x) } required() text "Output ivory repository."

    opt[Unit]('d', "dry-run")        action { (_, c) => c.copy(dry = true) } optional() text "Do a dry run only."

  }

  def run {
    parser.parse(args, CliArguments("", "", false)).map { c =>
      val rconf = RecreateConfig(Repository.fromHdfsPath(FilePath(c.input), configuration),
                                 Repository.fromHdfsPath(FilePath(c.output), configuration),
                                 configuration,
                                 Some(new SnappyCodec),
                                 false,
                                 c.dry)
      RecreateAction.all.run(rconf).run.unsafePerformIO.fold(
        ok => ok,
        err => { println(err); sys.exit(1) }
      )
    }
  }
}
