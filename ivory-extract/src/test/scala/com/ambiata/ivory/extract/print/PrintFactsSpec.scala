package com.ambiata.ivory.extract.print

import com.nicta.scoobi.core.ScoobiConfiguration
import com.nicta.scoobi.testing.{TempFiles, HadoopSpecification}
import com.nicta.scoobi.testing.TestFiles._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.extract._
import org.apache.hadoop.fs.Path
import org.joda.time.{LocalDate, DateTimeZone}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.testing.ResultMatcher.{beOk => beOkResult}
import com.ambiata.mundane.io._
import scalaz.effect.IO

class PrintFactsSpec extends HadoopSpecification with SampleFacts { def is = s2"""

 A sequence file containing facts can be read and printed on the console $a1

"""

  def a1 = { implicit sc: ScoobiConfiguration =>
    val directory = path(TempFiles.createTempDir("snapshot").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    val testDir = "target/"+getClass.getSimpleName+"/"
    val snapshot1 = HdfsSnapshot.takeSnapshot(repo.root.toHdfs, new Path(s"$testDir/out"), new Path(s"testDir/errors"), LocalDate.now, None)
    snapshot1.run(sc) must beOk

    val buffer = new StringBuffer
    val stringBufferLogging = (s: String) => IO { buffer.append(s+"\n"); ()}

    PrintFacts.printGlob(s"$testDir/out/thrift", "out-*", delim = "|", tombstone = "NA").execute(stringBufferLogging).unsafePerformIO must beOkResult

    buffer.toString must_==
      """|eid1|ns1|fid1|abc|2012-10-01|0:0:0
         |eid2|ns1|fid2|11|2012-11-01|0:0:0
         |eid3|ns2|fid3|true|2012-03-20|0:0:0
         |""".stripMargin
  }

  override def isCluster = false
}
