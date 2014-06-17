package com.ambiata.ivory.extract

import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.parse._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.mr._

import java.lang.{Iterable => JIterable}
import java.nio.ByteBuffer

import scalaz.{Reducer => _, _}, Scalaz._

import org.apache.hadoop.fs.{Path, FileSystem};
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.input.ProxyTaggedInputSplit
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.{TSerializer, TDeserializer}

import scala.collection.JavaConverters._

/*
 * This is a hand-coded MR job to squeeze the most out of snapshot performance.
 */
object SnapshotJob {
  def run(conf: Configuration, reducers: Int, date: Date, inputs: List[FactsetGlob], output: Path, incremental: Option[Path]): Unit = {
    // MR1
    conf.set("mapred.compress.map.output", "true")
    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec")

    // YARN
    conf.set("mapreduce.map.output.compress", "true")
    conf.set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")

    val job = Job.getInstance(conf)
    job.setJarByClass(classOf[SnapshotReducer])
    job.setJobName("ivory-snapshot")

    /* map */
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[BytesWritable]);

    /* partiton & sort */
    job.setGroupingComparatorClass(classOf[Text.Comparator])
    job.setSortComparatorClass(classOf[Text.Comparator])

    /* reducer */
    job.setNumReduceTasks(reducers)
    job.setReducerClass(classOf[SnapshotReducer])
    job.setOutputKeyClass(classOf[NullWritable]);
    job.setOutputValueClass(classOf[BytesWritable]);

    /* input */
    val mappers = inputs.map({
      case FactsetGlob(FactsetVersionOne, factsets) => (classOf[SnapshotFactsetVersionOneMapper], factsets)
      case FactsetGlob(FactsetVersionTwo, factsets) => (classOf[SnapshotFactsetVersionOneMapper], factsets)
    })
    mappers.foreach({ case (clazz, factsets) =>
      factsets.foreach({ case (_, ps) =>
        ps.foreach(p => MultipleInputs.addInputPath(job, new Path(p.path), classOf[SequenceFileInputFormat[_, _]], clazz))
      })
    })

    incremental.foreach(p =>
      MultipleInputs.addInputPath(job, p, classOf[SequenceFileInputFormat[_, _]], classOf[SnapshotIncrementalMapper]))

    /* output */
    val tmpout = new Path("/tmp/ivory-snapshot-" + java.util.UUID.randomUUID)
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK)
    FileOutputFormat.setCompressOutput(job, true)
    FileOutputFormat.setOutputCompressorClass(job, classOf[SnappyCodec])
    FileOutputFormat.setOutputPath(job, tmpout)

    /* cache / config initializtion */
    job.getConfiguration.set(Keys.SnapshotDate, date.int.toString)
    ThriftCache.push(job, Keys.FactsetLookup, priorityTable(inputs))

    /* run job */
    if (!job.waitForCompletion(true))
      sys.error("ivory snapshot failed.")

    /* commit files to factset */
    (for {
      _  <- Hdfs.mv(new Path(tmpout, "*"), output)
    } yield ()).run(conf).run.unsafePerformIO
  }

  def priorityTable(globs: List[FactsetGlob]): FactsetLookup = {
    val lookup = new FactsetLookup
    globs.foreach(_.factsets.foreach({ case (pfs, _) =>
      lookup.putToPriorities(pfs.set.name, pfs.priority.toShort)
    }))
    lookup
  }

  object Keys {
    val SnapshotDate = "ivory.snapdate"
    val FactsetLookup = ThriftCache.Key("factset-lookup")
  }
}

/*
 * Mappers for ivory-snapshot.
 *
 * The input is a standard SequenceFileInputFormat.
 *
 * The output key is a sting of entity|namespace|attribute
 *
 * The output value is the already serialized bytes of the NamespacedFact ready to write.
 */
class SnapshotFactsetVersionOneMapper extends Mapper[NullWritable, BytesWritable, Text, BytesWritable] {

  /* Value serializer/deserializer. */
  val serializer = new TSerializer(new TCompactProtocol.Factory)
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  /* Empty instance to use when deserialising */
  val tfact = new ThriftFact

  /* Snapshot date, see #setup. */
  var strDate: String = null
  lazy val date: Date = Date.fromInt(strDate.toInt).getOrElse(sys.error(s"Invalid snapshot date '${strDate}'"))

  val lookup = new FactsetLookup

  /* The output key, only create once per mapper. */
  val kout = new Text

  /* The output value, only create once per mapper. */
  val vout = new BytesWritable

  var partition: Partition = null

  var stringPath: String = null

  var priority: Short = 0

  override def setup(context: Mapper[NullWritable, BytesWritable, Text, BytesWritable]#Context): Unit = {
    strDate = context.getConfiguration.get(SnapshotJob.Keys.SnapshotDate)
    ThriftCache.pop(context.getConfiguration, SnapshotJob.Keys.FactsetLookup, lookup)
    vout.setCapacity(4096)
    stringPath = ProxyTaggedInputSplit.fromInputSplit(context.getInputSplit).getUnderlying.asInstanceOf[FileSplit].getPath.toString
    partition = Partition.parseWith(stringPath) match {
      case Success(p) => p
      case Failure(e) => sys.error(s"Can not parse partition ${e}")
    }
    priority = lookup.priorities.get(partition.factset.name)
  }

  override def map(key: NullWritable, value: BytesWritable, context: Mapper[NullWritable, BytesWritable, Text, BytesWritable]#Context): Unit = {
    deserializer.deserialize(tfact, value.getBytes)

    PartitionFactThriftStorageV1.parseFact(stringPath, tfact) match {
      case \/-(f) =>
        context.getCounter("ivory", "snapshot.v1.ok").increment(1)

        if(f.date > date)
          context.getCounter("ivory", "snapshot.v1.skip").increment(1)
        else {
          val k = s"${f.entity}|${f.namespace}|${f.feature}"
          kout.set(k)

          val factbytes = serializer.serialize(f.toNamespacedThrift)
          val v = serializer.serialize(new PrioritizedFactBytes(priority, ByteBuffer.wrap(factbytes)))
          vout.set(v, 0, v.length)

          context.write(kout, vout)
        }
      case -\/(e) =>
        sys.error(s"Can not read fact - ${e}")
    }
  }
}

class SnapshotIncrementalMapper extends Mapper[NullWritable, BytesWritable, Text, BytesWritable] {

  /* Value serializer/deserializer. */
  val serializer = new TSerializer(new TCompactProtocol.Factory)
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  /* Empty instance to use when deserialising */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  /* The output key, only create once per mapper. */
  val kout = new Text

  /* The output value, only create once per mapper. */
  val vout = new BytesWritable

  val priority = Priority.Max.toShort

  override def setup(context: Mapper[NullWritable, BytesWritable, Text, BytesWritable]#Context): Unit = {
    vout.setCapacity(4096)
  }

  override def map(key: NullWritable, value: BytesWritable, context: Mapper[NullWritable, BytesWritable, Text, BytesWritable]#Context): Unit = {

    context.getCounter("ivory", "snapshot.incr.ok").increment(1)
    
    deserializer.deserialize(fact, value.getBytes)
    kout.set(s"${fact.entity}|${fact.namespace}|${fact.feature}")

    val v = serializer.serialize(new PrioritizedFactBytes(priority, ByteBuffer.wrap(value.getBytes)))
    vout.set(v, 0, v.length)

    context.write(kout, vout)
  }
}


/*
 * Reducer for ivory-snapshot.
 *
 * This reducer takes the latest fact from entity|namespace|attribute
 *
 * The input value is the bytes representation of the fact ready to write out.
 *
 * The output is a sequence file, with no key, and the bytes of the serialized NamespacedFact.
 */
class SnapshotReducer extends Reducer[Text, BytesWritable, NullWritable, BytesWritable] {

  val deserializer = new TDeserializer(new TCompactProtocol.Factory)
  
  val container = new PrioritizedFactBytes

  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  val vout = {
    val bw = new BytesWritable
    bw.setCapacity(4096)
    bw
  }

  override def reduce(key: Text, iter: JIterable[BytesWritable], context: Reducer[Text, BytesWritable, NullWritable, BytesWritable]#Context): Unit = {
    val iterator = iter.iterator
    var latestContainer: PrioritizedFactBytes = null
    var latestDate: Int = 0
    while (iterator.hasNext) {
      val next = iterator.next
      deserializer.deserialize(container, next.getBytes)
      deserializer.deserialize(fact, container.getFactbytes)
      val nextDate = fact.getYyyyMMdd
      if(latestContainer == null || nextDate > latestDate || (nextDate == latestDate && container.getPriority < latestContainer.getPriority)) {
        latestContainer = container
        latestDate = nextDate
      }
    }
    vout.set(latestContainer.getFactbytes, 0, latestContainer.getFactbytes.length)
    context.write(NullWritable.get, vout)
  }
}
