package com.ambiata.ivory.extract

import com.nicta.scoobi.testing.{TempFiles, HadoopSpecification}
import com.nicta.scoobi.testing.TestFiles._
import com.ambiata.ivory.extract._
import com.ambiata.ivory.core.Repository
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import com.nicta.scoobi.core.ScoobiConfiguration

class SnapshotSpec extends HadoopSpecification with SampleFacts { def is = s2"""
                                                         
  A snapshot of the features can be extracted as a sequence file $e1

"""

  def e1 = { implicit sc: ScoobiConfiguration =>
    val directory = path(TempFiles.createTempDir("chord").getPath)
    val repo = Repository.fromHdfsPath(new Path(directory + "/repo"))

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    val testDir = "target/"+getClass.getSimpleName+"/"
    val snapshot1 = HdfsSnapshot.takeSnapshot(repo.path, new Path(s"$testDir/out"), new Path(s"testDir/errors"), LocalDate.now, None)
    ok
  }

  override def isCluster = false
}
