package com.ambiata.ivory.extract

import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.{HadoopSpecification, TempFiles}
import com.ambiata.ivory.core._
import org.apache.hadoop.fs.{Path}
import org.joda.time.LocalDate
import com.nicta.scoobi.core.ScoobiConfiguration
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import IvoryStorage._
import ScoobiAction._
import scalaz._, Scalaz._
import com.ambiata.ivory.alien.hdfs.Hdfs

class PivotSpec extends HadoopSpecification with SampleFacts { def is = s2"""

 A Sequence file containing feature values can be pivoted as a row-oriented file with a new dictionary $e1

"""

  def e1 = { implicit sc: ScoobiConfiguration =>
    val directory = path(TempFiles.createTempDir("chord").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    val testDir = "target/"+getClass.getSimpleName+"/"
    val errors = new Path(s"$testDir/errors")
    val takeSnapshot = HdfsSnapshot.takeSnapshot(repo.root.toHdfs, errors, Date.fromLocalDate(LocalDate.now), false, None)

    val pivot = new Path(s"$testDir/pivot")
    val action =
      takeSnapshot >>
      fromHdfs(dictionaryFromIvory(repo, DICTIONARY_NAME)).flatMap { dictionary =>
        Pivot.onHdfsWithDictionary(new Path(repo.snapshots.toHdfs, "00000000"), pivot, errors, dictionary, '|', "NA")
      } >> fromHdfs(Hdfs.globLines(pivot, "out*"))

    action.run(sc) must beOkLike { lines =>
      lines.mkString("\n").trim must_==
      """|eid1|abc|NA|NA
         |eid2|NA|11|NA
         |eid3|NA|NA|true
      """.stripMargin.trim
    }

  }

  override def isCluster = false
}
