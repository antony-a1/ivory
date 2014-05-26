package com.ambiata.ivory.storage.legacy

import scalaz._, Scalaz._

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io.Streams
import com.ambiata.mundane.parse.ListParser

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._

case class SnapshotMeta(date: Date, store: String) {

  def toHdfs(path: Path): Hdfs[Unit] =
    Hdfs.writeWith(path, os => Streams.write(os, stringLines))

  lazy val stringLines: String =
    date.string("-") + "\n" + store
}

object SnapshotMeta {

  val fname = ".snapmeta"

  def fromHdfs(path: Path): Hdfs[SnapshotMeta] = for {
    raw <- Hdfs.readWith(path, is => Streams.read(is))
    sm  <- Hdfs.fromValidation(parser.run(raw.lines.toList))
  } yield sm

  def parser: ListParser[SnapshotMeta] = {
    import ListParser._
    for {
      d <- localDate
      s <- string.nonempty
    } yield SnapshotMeta(Date.fromLocalDate(d), s)
  }

  def latest(snapshots: Path, date: Date): Hdfs[Option[(Path, SnapshotMeta)]] = for {
    paths <- Hdfs.globPaths(snapshots)
    metas <- paths.traverse(p => {
      val snapmeta = new Path(p, fname)
      Hdfs.exists(snapmeta).flatMap(e =>
        if(e) fromHdfs(snapmeta).map[Option[(Path, SnapshotMeta)]](sm => Some((p, sm))) else Hdfs.value(None))
    }).map(_.flatten)
  } yield metas.filter(_._2.date.isBeforeOrEqual(date)).sortBy(_._2.date).lastOption
}
