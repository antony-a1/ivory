package com.ambiata.ivory.extract

import com.nicta.scoobi.testing.{TempFiles, HadoopSpecification}
import com.nicta.scoobi.testing.TestFiles._
import com.ambiata.mundane.io._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.extract._
import com.ambiata.ivory.storage.repository._
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import com.nicta.scoobi.core.ScoobiConfiguration

class SnapshotSpec extends HadoopSpecification with SampleFacts { def is = s2"""

  A snapshot of the features can be extracted as a sequence file $e1

"""

  def e1 = { implicit sc: ScoobiConfiguration =>
    val directory = path(TempFiles.createTempDir("snapshot").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    val testDir = "target/"+getClass.getSimpleName+"/"
    val snapshot1 = HdfsSnapshot.takeSnapshot(repo.root.toHdfs, new Path(s"testDir/errors"), LocalDate.now, false)
    ok
  }

  override def isCluster = false
}
