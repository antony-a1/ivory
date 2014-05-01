package com.ambiata.ivory.extract

import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.{HadoopSpecification, TempFiles}
import com.ambiata.ivory.core._
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import com.nicta.scoobi.core.ScoobiConfiguration
import com.ambiata.mundane.testing.ResultTIOMatcher._

class PivotSpec extends HadoopSpecification with SampleFacts { def is = s2"""

 A Sequence file containing feature values can be pivoted as a row-oriented file with a new dictionary $e1

"""

  def e1 = { implicit sc: ScoobiConfiguration =>
    val directory = path(TempFiles.createTempDir("chord").getPath)
    val repo = Repository.fromHdfsPath(new Path(directory + "/repo"))

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)

    val testDir = "target/"+getClass.getSimpleName+"/"
    val snapshot = new Path(s"$testDir/snapshot")
    val errors = new Path(s"testDir/errors")
    val output = new Path(s"testDir/output")
    val dictionary = new Path(s"testDir/dictionary")
    val snapshot1 = HdfsSnapshot.takeSnapshot(repo.path, snapshot, errors, LocalDate.now, None)

    Pivot.onHdfs(snapshot, output, errors, dictionary, '|', "NA").run(sc) must beOk
  }.pendingUntilFixed

}
