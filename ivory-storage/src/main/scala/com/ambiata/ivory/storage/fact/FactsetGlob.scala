package com.ambiata.ivory.storage.fact
/*
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.conf.Configuration

object FactsetGlob {
  def expandGlob(path: String, from: Option[Date], to: Option[Date])(implicit sc: ScoobiConfiguration): List[String] =
    (from, to) match {
      case (None, None)         => List(path)
      case (None, Some(td))     => expandGlobWith(path, p => Partitions.pathsBeforeOrEqual(p, td))
      case (Some(fd), None)     => expandGlobWith(path, p => Partitions.pathsAfterOrEqual(p, fd))
      case (Some(fd), Some(td)) => expandGlobWith(path, p => Partitions.pathsBetween(p, fd, td))
    }

  def expandGlobWith(path: String, f: List[Partition] => List[Partition])(implicit sc: ScoobiConfiguration): List[String] = (for {
    paths      <- Hdfs.globFiles(new Path(path))
    partitions <- paths.traverse(p => Hdfs.fromValidation(Partition.parseWith(p.toString)))
  } yield f(partitions)).run(sc).run.unsafePerformIO() match {
    case Error(e) => sys.error(s"Could not access hdfs - ${e}")
    case Ok(glob) => glob.map(_.path)
  }
}

*/
