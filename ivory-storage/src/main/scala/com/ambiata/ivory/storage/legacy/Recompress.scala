package com.ambiata.ivory.storage.legacy


import com.ambiata.mundane.io._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.alien.hdfs._, HdfsS3Action._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.nicta.scoobi.{core, Scoobi}, Scoobi._
import com.nicta.scoobi.core.{Emitter => _, _}
import com.nicta.scoobi.io.sequence._
import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import com.nicta.scoobi.impl.util.DistCache

import java.io.{DataInput, DataOutput}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.Scoobi.DList
import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import com.nicta.scoobi.Scoobi.WireFormat
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scalaz._, Scalaz._
import scala.collection.JavaConverters._

case class Stat(in: String, out: String, size: Long, seq: Boolean) {
  def path = new Path(out)
  def target = new Path(out)
}

object Recompress {
  def select(input: Path, output: Path, seq: Boolean): Hdfs[List[Stat]] =
    Hdfs.globFilesRecursively(input).flatMap(_.traverse(a => Hdfs.size(a).map(s => {
      val in = a
      println("A: " + in.toString)
      println("B: " + in.toString.replace(input.toString + "/", ""))
      println("C: " + new Path(output, in.toString.replace(input.toString + "/", "")))
      val out = new Path(output, in.toString.replace(input.toString + "/", ""))
      Stat(in.toString, out.toString, s, seq && !in.getName.startsWith(".") && !in.getName.startsWith("_"))
   })))

  def all(input: String, output: String): Hdfs[List[Stat]] = for {
    errors <- select(new Path(input, "errors"), new Path(output, "errors"), false)
    factsets <- select(new Path(input, "factsets"), new Path(output, "factsets"), true)
    metadata <- select(new Path(input, "metadata"), new Path(output, "metadata"), false)
    snapshots <- select(new Path(input, "snapshots"), new Path(output, "snapshots"), true)
  } yield errors ++ factsets ++ metadata ++ snapshots

  def go(input: String, output: String, distribution: Int, dry: Boolean): ScoobiAction[Unit] = for {
    stats <- ScoobiAction.fromHdfs { all(input, output) }
    _     <- if (dry) print(stats) else gox(stats, distribution)
  } yield ()

  def print(stats: List[Stat]): ScoobiAction[Unit] = ScoobiAction.safe {
    stats.foreach(stat =>
      println(s"""${stat.in} -> ${stat.out} ${if (stat.seq) "[compress]" else ""}""")
    )
  }

  def gox(stats: List[Stat], distribution: Int): ScoobiAction[Unit] =
    ScoobiAction.scoobiConfiguration.map(c =>
      run(fromSource(new PathSource(stats, distribution, classOf[PathInputFormat])).parallelDo((stat: Stat, emmitter: Emitter[Unit]) => {
        if (stat.seq)
          facts(stat.path, stat.target)
        else
          Hdfs.cp(stat.path, stat.target, false).run(new Configuration).run.unsafePerformIO.toOption.getOrElse(sys.error("Couldn't copy: " + stat.path))
      }))(c)
  )

  //******************************** Ignore below here for now, this is based on the dist-copy related code, can be factored out
  //                                 when that is ready.

  def facts(from: Path, to: Path): Unit = {
    val reader = new SequenceFile.Reader(new Configuration, SequenceFile.Reader.file(from))
    val opts = List(
      SequenceFile.Writer.file(to),
      SequenceFile.Writer.keyClass(classOf[NullWritable]),
      SequenceFile.Writer.valueClass(classOf[BytesWritable]),
      SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new SnappyCodec))
    val writer = SequenceFile.createWriter(new Configuration, opts:_*)
    val bytes = new BytesWritable
    val nul = NullWritable.get
    try {
      while (reader.next(nul, bytes))
        writer.append(nul, bytes)
    } finally {
      writer.close
      reader.close
    }
  }

  // naive greedy partitioning strategy /co Eric.
  def partition[A](items: List[A], n: Int, sizeOf: A => Long): List[List[A]] =
    items.sortWith(maxSize(sizeOf)).foldLeft(Vector.fill(n)(Vector[A]())) { (res, cur) =>
      res.sortBy(_.map(sizeOf).map(l => BigInt(l)).sum) match {
        case head +: rest => (cur +: head) +: rest
        case empty        => Vector(Vector(cur))
      }
    }.map(_.toList).toList

  def maxSize[A](sizeOf: A => Long): (A, A) => Boolean =
    (a, b) => sizeOf(a) > sizeOf(b)
}


object Constants {
  val DIST_SYNC      = "scoobi.ivory.recompress"
  val MAPPERS_NUMBER = DIST_SYNC+"mappersnumber"
  val FILES          = DIST_SYNC+"files"
}
class PathSource(val paths: List[Stat], val mappersNumber: Int, val inputFormat: Class[PathInputFormat])
    extends DataSource[NullWritable, Stat, Stat] {
  import Constants._

  val inputConverter = new InputConverter[NullWritable, Stat, Stat] {
    def fromKeyValue(context: InputContext, k: NullWritable, v: Stat) = v
  }

  override def toString = "PathSource("+id+")"

  def inputCheck(implicit sc: ScoobiConfiguration) {}

  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    DistCache.pushObject[List[Stat]](sc.configuration, paths, FILES)
    sc.set(MAPPERS_NUMBER, mappersNumber)
  }

  def inputSize(implicit sc: ScoobiConfiguration): Long = paths.map(_.size).sum
}

class PathInputFormat extends InputFormat[NullWritable, Stat] {
  import Constants._

  def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val configuration = context.getConfiguration
    val splits =
      DistCache.pullObject[List[Stat]](configuration, FILES, memoise = true).map { paths =>
        val mappersNumber = context.getConfiguration.getInt(MAPPERS_NUMBER, 1)
        Recompress.partition(paths, mappersNumber, (_: Stat).size).map(x => FilesSplit(x): InputSplit)
      }.getOrElse(Nil)
    splits.asJava
  }

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, Stat] = new RecordReader[NullWritable, Stat] {
    private var files: FilesSplit = FilesSplit(List())
    private var configuration: Configuration = new Configuration
    private var fileNumber = 0

    def initialize(split: InputSplit, context: TaskAttemptContext) = {
      files = split.asInstanceOf[FilesSplit]
      configuration = context.getConfiguration
    }

    def nextKeyValue: Boolean = fileNumber != files.size
    def getCurrentKey: NullWritable = NullWritable.get
    def getCurrentValue: Stat = {
      val file = files(fileNumber)
      fileNumber += 1
      file
    }
    def getProgress: Float = if (files.size == 0) 1 else fileNumber / files.size
    def close {}
  }
}


case class FilesSplit(var paths: List[Stat]) extends InputSplit with Writable {
  def this() = this(List())
  def apply(i: Int) = paths(i)
  def size = paths.size
  def getLength: Long = paths.map(_.size).sum
  def getLocations: Array[String] = Array()

  def write(out: DataOutput) = {
    out.writeInt(paths.size)
    paths.foreach(p => Stat.wireFormat.write(p, out))
  }
  def readFields(in: DataInput) = {
    val size = in.readInt
    paths = (1 to size).map(_ => Stat.wireFormat.read(in)).toList
  }

}

object FilesSplit {
  def wireFormat: WireFormat[FilesSplit] =
    mkCaseWireFormat(FilesSplit.apply _, FilesSplit.unapply _)
}

object Stat {
  implicit def wireFormat: WireFormat[Stat] =
    mkCaseWireFormat(Stat.apply _, Stat.unapply _)
}
