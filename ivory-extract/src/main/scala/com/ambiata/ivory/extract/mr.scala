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
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.{TSerializer, TDeserializer}

/*
 * This is a hand-coded MR job to squeeze the most out of snapshot performance.
 */
object SnapshotJob {
  def run(conf: Configuration, reducers: Int, date: Date, inputs: List[FactsetGlob], output: Path, incremental: Option[Path], codec: Option[CompressionCodec]): Unit = {

    val job = Job.getInstance(conf)
    val ctx = MrContext.newContext("ivory-snapshot", job)

    job.setJarByClass(classOf[SnapshotReducer])
    job.setJobName("ivory-snapshot")

    /* map */
    job.setMapOutputKeyClass(classOf[BytesWritable]);
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
      case FactsetGlob(FactsetVersionTwo, factsets) => (classOf[SnapshotFactsetVersionTwoMapper], factsets)
    })
    mappers.foreach({ case (clazz, factsets) =>
      factsets.foreach({ case (_, ps) =>
        ps.foreach(p => {
          println(s"Input path: ${p.path}")
          MultipleInputs.addInputPath(job, new Path(p.path), classOf[SequenceFileInputFormat[_, _]], clazz)
        })
      })
    })

    incremental.foreach(p => {
      println(s"Incremental path: ${p}")
      MultipleInputs.addInputPath(job, p, classOf[SequenceFileInputFormat[_, _]], classOf[SnapshotIncrementalMapper])
    })

    /* output */
    val tmpout = new Path(ctx.output, "snap")
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
    FileOutputFormat.setOutputPath(job, tmpout)

    /* compression */
    codec.foreach(cc => {
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    })

    /* cache / config initializtion */
    job.getConfiguration.set(Keys.SnapshotDate, date.int.toString)
    ctx.thriftCache.push(job, Keys.FactsetLookup, priorityTable(inputs))

    /* run job */
    if (!job.waitForCompletion(true))
      sys.error("ivory snapshot failed.")

    /* commit files to factset */
    Committer.commit(ctx, {
      case "snap" => output
    }, true).run(conf).run.unsafePerformIO
  }

  def priorityTable(globs: List[FactsetGlob]): FactsetLookup = {
    val lookup = new FactsetLookup
    globs.foreach(_.factsets.foreach({ case (pfs, _) =>
      lookup.putToPriorities(pfs.set.name, pfs.priority.toShort)
    }))
    lookup
  }

  private val keyBytes = new Array[Byte](4096)

  /* Create an output key by mutating a BytesWritable with the bytes from
     entity/namespace/feature */
  @inline
  def populateKey(f: Fact, bytes: BytesWritable) {
    val b1 = f.entity.getBytes
    val b2 = f.namespace.getBytes
    val b3 = f.feature.getBytes
    System.arraycopy(b1, 0, keyBytes, 0, b1.length)
    System.arraycopy(b2, 0, keyBytes, b1.length, b2.length)
    System.arraycopy(b3, 0, keyBytes, b1.length + b2.length, b3.length)
    bytes.set(keyBytes, 0, b1.length + b2.length + b3.length)
  }

  object Keys {
    val SnapshotDate = "ivory.snapdate"
    val FactsetLookup = ThriftCache.Key("factset-lookup")
  }
}

/*
 * Base mapper for ivory-snapshot.
 *
 * The input is a standard SequenceFileInputFormat. The path is used to determin the
 * factset/namespace/year/month/day, and a factset priority is pull out of a lookup
 * table in the distributes cache.
 *
 * The output key is a sting of entity|namespace|attribute
 *
 * The output value is expected (can not be typed checked because its all bytes) to be
 * a thrift serialized PriorityTag object. This is a container that holds a
 * factset priority and thrift serialized NamespacedFact object.
 */
abstract class SnapshotFactseBaseMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {

  /* Context object holding dist cache paths */
  var ctx: MrContext = null

  /* Thrift serializer/deserializer. */
  val serializer = new TSerializer(new TCompactProtocol.Factory)
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  /* Empty instance to use when deserialising */
  val tfact = new ThriftFact

  /* empty conainter class used to populate priority and serialized fact. This is mutated per record */
  val container = new PriorityTag

  /* Snapshot date, see #setup. */
  var strDate: String = null
  var date: Date = Date.unsafeFromInt(0)

  /* Lookup table for facset priority */
  val lookup = new FactsetLookup

  /* The output key, only create once per mapper. */
  val kout = new BytesWritable

  /* The output value, only create once per mapper. */
  val vout = new BytesWritable

  /* Partition created from input split path, only created once per mapper */
  var partition: Partition = null

  /* Input split path, only created once per mapper */
  var stringPath: String = null

  /* Priority of the factset, only created once per record */
  var priority: Short = 0

  override def setup(context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context): Unit = {
    kout.setCapacity(4096)
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    strDate = context.getConfiguration.get(SnapshotJob.Keys.SnapshotDate)
    date = Date.fromInt(strDate.toInt).getOrElse(sys.error(s"Invalid snapshot date '${strDate}'"))
    ctx.thriftCache.pop(context.getConfiguration, SnapshotJob.Keys.FactsetLookup, lookup)
    stringPath = MrContext.getSplitPath(context.getInputSplit).toString
    partition = Partition.parseWith(stringPath) match {
      case Success(p) => p
      case Failure(e) => sys.error(s"Can not parse partition ${e}")
    }
    priority = lookup.priorities.get(partition.factset.name)
  }
}

/**
 * FactsetVersionOne mapper
 */
class SnapshotFactsetVersionOneMapper extends SnapshotFactseBaseMapper {
  override def map(key: NullWritable, value: BytesWritable, context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context): Unit = {
    deserializer.deserialize(tfact, value.getBytes)

    PartitionFactThriftStorageV1.parseFact(stringPath, tfact) match {
      case \/-(f) =>
        context.getCounter("ivory", "snapshot.v1.ok").increment(1)

        if(f.date > date)
          context.getCounter("ivory", "snapshot.v1.skip").increment(1)
        else {
          SnapshotJob.populateKey(f, kout)

          val factbytes = serializer.serialize(f.toNamespacedThrift)
          container.clear()
          container.setPriority(priority)
          container.setFactbytes(ByteBuffer.wrap(factbytes))
          val v = serializer.serialize(container)
          vout.set(v, 0, v.length)

          context.write(kout, vout)
        }
      case -\/(e) =>
        sys.error(s"Can not read fact - ${e}")
    }
  }
}

/**
 * FactsetVersionTwo mapper
 */
class SnapshotFactsetVersionTwoMapper extends SnapshotFactseBaseMapper {
  override def map(key: NullWritable, value: BytesWritable, context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context): Unit = {
    deserializer.deserialize(tfact, value.getBytes)

    PartitionFactThriftStorageV2.parseFact(stringPath, tfact) match {
      case \/-(f) =>
        context.getCounter("ivory", "snapshot.v2.ok").increment(1)

        if(f.date > date)
          context.getCounter("ivory", "snapshot.v2.skip").increment(1)
        else {
          SnapshotJob.populateKey(f, kout)

          val factbytes = serializer.serialize(f.toNamespacedThrift)
          container.clear()
          container.setPriority(priority)
          container.setFactbytes(ByteBuffer.wrap(factbytes))
          val v = serializer.serialize(container)
          vout.set(v, 0, v.length)

          context.write(kout, vout)
        }
      case -\/(e) =>
        sys.error(s"Can not read fact - ${e}")
    }
  }
}

/**
 * Incremental snapshot mapper.
 */
class SnapshotIncrementalMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {

  /* Thrift serializer/deserializer. */
  val serializer = new TSerializer(new TCompactProtocol.Factory)
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  /* Empty instance to use when deserialising */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  /* empty conainter class used to populate priority and serialized fact. This is mutated per record */
  val container = new PriorityTag

  /* The output key, only create once per mapper. */
  val kout = new BytesWritable

  /* The output value, only create once per mapper. */
  val vout = new BytesWritable

  /* Priority of the incremental is always Priority.Max */
  val priority = Priority.Max.toShort

  override def setup(context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context): Unit = {
    kout.setCapacity(4096)
    vout.setCapacity(4096)
  }

  override def map(key: NullWritable, value: BytesWritable, context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context): Unit = {
    context.getCounter("ivory", "snapshot.incr.ok").increment(1)
    
    val size = value.getLength
    val bytes = new Array[Byte](size)
    System.arraycopy(value.getBytes, 0, bytes, 0, size)

    deserializer.deserialize(fact, bytes)
    SnapshotJob.populateKey(fact, kout)

    container.clear()
    container.setPriority(priority)
    container.setFactbytes(ByteBuffer.wrap(bytes))
    val v = serializer.serialize(container)
    vout.set(v, 0, v.length)

    context.write(kout, vout)
  }
}


/*
 * Reducer for ivory-snapshot.
 *
 * This reducer takes the latest fact with the same entity|namespace|attribute key
 *
 * The input values are serialized conainers of factset priority and bytes of serialized NamespacedFact.
 *
 * The output is a sequence file, with no key, and the bytes of the serialized NamespacedFact.
 */
class SnapshotReducer extends Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] {

  /* Thrift deserializer. */
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)
  
  /* empty conainter class used to populate deserialized values. This is mutated per record */
  val container = new PriorityTag

  /* empty fact class used to populate deserialized facts. This is mutated per record */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  /* The output value, only create once per mapper. */
  val vout = new BytesWritable

  override def setup(context: Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit = {
    vout.setCapacity(4096)
  }

  override def reduce(key: BytesWritable, iter: JIterable[BytesWritable], context: Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit = {

    /****************** !!!!!! WARNING !!!!!! ******************
     *
     * This is some nasty nasty mutation that can coorrupt data
     * without knowing, so double/triple check with others when
     * changing.
     *
     ***********************************************************/
    val iterator = iter.iterator

    // use one object to hold state instead of three
    var latestContainer: PriorityTag = null
    var latestDate = 0l
    var isTombstone = true
    while (iterator.hasNext) {
      val next = iterator.next
      deserializer.deserialize(container, next.getBytes)
      deserializer.deserialize(fact, container.getFactbytes) // explain why deserialize twice
      val nextDate = fact.datetime.long
      // move the if statement to a function
      if(latestContainer == null || nextDate > latestDate || (nextDate == latestDate && container.getPriority < latestContainer.getPriority)) {
        // change to state.set (1 line)
        latestContainer = container.deepCopy
        latestDate = nextDate
        isTombstone = fact.isTombstone
      }
    }
    // change to state.write
    if(!isTombstone) {
      vout.set(latestContainer.getFactbytes, 0, latestContainer.getFactbytes.length)
      context.write(NullWritable.get, vout)
    }
  }
}
