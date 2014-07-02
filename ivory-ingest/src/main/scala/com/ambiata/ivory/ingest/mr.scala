package com.ambiata.ivory.ingest

import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.parse._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.mr._

import java.lang.{Iterable => JIterable}

import scalaz.{Reducer => _, _}, Scalaz._

import org.apache.hadoop.fs.{Path, FileSystem};
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.TSerializer

import org.joda.time.DateTimeZone

/*
 * This is a hand-coded MR job to squeeze the most out of ingestion performance.
 */
object IngestJob {
  // FIX shouldn't need `root: Path` it is a workaround for poor namespace handling
  def run(conf: Configuration, reducers: Int, allocations: ReducerLookup, namespaces: NamespaceLookup, features: FeatureIdLookup, dict: Dictionary, ivoryZone: DateTimeZone, ingestZone: DateTimeZone, root: Path, paths: List[String], target: Path, errors: Path, codec: Option[CompressionCodec]): Unit = {

    val job = Job.getInstance(conf)
    val ctx = MrContext.newContext("ivory-ingest", job)

    job.setJarByClass(classOf[IngestMapper])
    job.setJobName("ivory-ingest")

    /* map */
    job.setMapperClass(classOf[IngestMapper])
    job.setMapOutputKeyClass(classOf[LongWritable]);
    job.setMapOutputValueClass(classOf[BytesWritable]);

    /* partition & sort */
    job.setPartitionerClass(classOf[IngestPartitioner])
    job.setGroupingComparatorClass(classOf[LongWritable.Comparator])
    job.setSortComparatorClass(classOf[LongWritable.Comparator])

    /* reducer */
    job.setNumReduceTasks(reducers)
    job.setReducerClass(classOf[IngestReducer])

    /* input */
    job.setInputFormatClass(classOf[TextInputFormat])
    FileInputFormat.addInputPaths(job, paths.mkString(","))

    /* output */
    LazyOutputFormat.setOutputFormatClass(job, classOf[SequenceFileOutputFormat[_, _]])
    MultipleOutputs.addNamedOutput(job, Keys.Out,  classOf[SequenceFileOutputFormat[_, _]],  classOf[NullWritable], classOf[BytesWritable]);
    MultipleOutputs.addNamedOutput(job, Keys.Err,  classOf[SequenceFileOutputFormat[_, _]],  classOf[NullWritable], classOf[BytesWritable]);
    FileOutputFormat.setOutputPath(job, ctx.output)

    /* compression */
    codec.foreach(cc => {
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    })

    /* cache / config initializtion */
    ctx.thriftCache.push(job, Keys.NamespaceLookup, namespaces)
    ctx.thriftCache.push(job, Keys.FeatureIdLookup, features)
    ctx.thriftCache.push(job, Keys.ReducerLookup, allocations)
    ctx.textCache.push(job, Keys.Dictionary, DictionaryTextStorage.delimitedDictionaryString(dict, '|'))
    job.getConfiguration.set(Keys.IvoryZone, ivoryZone.getID)
    job.getConfiguration.set(Keys.IngestZone, ingestZone.getID)
    job.getConfiguration.set(Keys.IngestBase, FileSystem.get(conf).getFileStatus(root).getPath.toString)

    /* run job */
    if (!job.waitForCompletion(true))
      sys.error("ivory ingest failed.")

    /* commit files to factset */
    Committer.commit(ctx, {
      case "errors"  => errors
      case "factset" => target
    }, true).run(conf).run.unsafePerformIO
  }

  def index(dict: Dictionary): (NamespaceLookup, FeatureIdLookup) = {
    val namespaces = new NamespaceLookup
    val features = new FeatureIdLookup
    dict.meta.toList.zipWithIndex.foreach({ case ((fid, _), idx) =>
      namespaces.putToNamespaces(idx, fid.namespace)
      features.putToIds(fid.toString, idx)
    })
    (namespaces, features)
  }

  def partitionFor(lookup: NamespaceLookup, key: LongWritable): String =
    "factset" + "/" + lookup.namespaces.get((key.get >>> 32).toInt) + "/" + Date.unsafeFromInt((key.get & 0xffffffff).toInt).slashed + "/part"

  object Keys {
    val NamespaceLookup = ThriftCache.Key("namespace-lookup")
    val FeatureIdLookup = ThriftCache.Key("feature-id-lookup")
    val ReducerLookup = ThriftCache.Key("reducer-lookup")
    val Dictionary = TextCache.Key("dictionary")
    val IvoryZone = "ivory.tz"
    val IngestZone = "ivory.ingest.tz"
    val IngestBase = "ivory.ingest.base"
    val Out = "out"
    val Err = "err"
  }
}

/**
 * Partitioner for ivory-ingest.
 *
 * Keys are partitioned by the externalized feature id (held in the top 32 bits of the key)
 * into predetermined buckets. We use the predetermined buckets as upfront knowledge of
 * the input size is used to reduce skew on input data.
 */
class IngestPartitioner extends Partitioner[LongWritable, BytesWritable] with Configurable {
  var _conf: Configuration = null
  var ctx: MrContext = null
  val lookup = new ReducerLookup

  def setConf(conf: Configuration): Unit = {
    _conf = conf
    ctx = MrContext.fromConfiguration(_conf)
    ctx.thriftCache.pop(conf, IngestJob.Keys.ReducerLookup, lookup)
  }

  def getConf: Configuration =
    _conf

  def getPartition(k: LongWritable, v: BytesWritable, partitions: Int): Int =
    lookup.reducers.get((k.get >>> 32).toInt) % partitions
}

/*
 * Mapper for ivory-ingest.
 *
 * The input is a standard TextInputFormat.
 *
 * The output key is a long, where the top 32 bits is an externalized feature id and
 * the bottom 32 bits is an ivory date representation of the yyyy-MM-dd of the fact.
 *
 * The output value is the already serialized bytes of the fact ready to write.
 */
class IngestMapper extends Mapper[LongWritable, Text, LongWritable, BytesWritable] {
  /* Context object contains tmp paths and dist cache */
  var ctx: MrContext = null

  /* Cache for path -> namespace mapping. */
  var namespaces: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty

  /* Value serializer. */
  val serializer = new TSerializer(new TCompactProtocol.Factory)

  /* Error output channel, see #setup */
  var out: MultipleOutputs[NullWritable, BytesWritable] = null

  /* FeatureId.toString -> Int mapping for externalizing feature id, see #setup */
  val lookup = new FeatureIdLookup

  /* Dictionary for this load, see #setup. */
  var dict: Dictionary = null

  /* Ivory repository time zone, see #setup. */
  var ivoryZone: DateTimeZone = null

  /* Ingestion time zone, see #setup. */
  var ingestZone: DateTimeZone = null

  /* Base path for this load, used to determine namespace, see #setup.
     FIX: this is hacky as (even more than the rest) we should rely on
     a marker file or something more sensible that a directory name. */
  var base: String = null

  /* The output key, only create once per mapper. */
  val kout = new LongWritable

  /* The output value, only create once per mapper. */
  val vout = new BytesWritable

  /* Path this mapper is processing */
  var splitPath: Path = null

  override def setup(context: Mapper[LongWritable, Text, LongWritable, BytesWritable]#Context): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    out = new MultipleOutputs(context.asInstanceOf[Mapper[LongWritable, Text, NullWritable, BytesWritable]#Context])
    ctx.thriftCache.pop(context.getConfiguration, IngestJob.Keys.FeatureIdLookup, lookup)
    dict = ctx.textCache.pop(context.getConfiguration, IngestJob.Keys.Dictionary, DictionaryTextStorage.fromString)
    ivoryZone = DateTimeZone.forID(context.getConfiguration.get(IngestJob.Keys.IvoryZone))
    ingestZone = DateTimeZone.forID(context.getConfiguration.get(IngestJob.Keys.IngestZone))
    base = context.getConfiguration.get(IngestJob.Keys.IngestBase)
    splitPath = MrContext.getSplitPath(context.getInputSplit)
    vout.setCapacity(4096)
  }

  override def cleanup(context: Mapper[LongWritable, Text, LongWritable, BytesWritable]#Context): Unit =
    out.close()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, LongWritable, BytesWritable]#Context): Unit = {
    val line = value.toString

    EavtParsers.parse(line, dict, namespaces.getOrElseUpdate(splitPath.getParent.toString, findIt(splitPath)), ingestZone) match {
      case Success(f) =>

        context.getCounter("ivory", "ingest.ok").increment(1)

        val k = lookup.ids.get(f.featureId.toString).toInt
        kout.set((k.toLong << 32) | f.date.int.toLong)

        val v = serializer.serialize(f.toThrift)
        vout.set(v, 0, v.length)

        context.write(kout, vout)

      case Failure(e) =>

        context.getCounter("ivory", "ingest.error").increment(1)

        val v = serializer.serialize(new ThriftParseError(line, e))
        vout.set(v, 0, v.length)

        out.write(IngestJob.Keys.Err, NullWritable.get, vout, "errors/part")
    }
  }

  def findIt(p: Path): String =
    if (p.getParent.toString == base)
      p.getName
    else
      findIt(p.getParent)
}


/*
 * Reducer for ivory-ingest.
 *
 * This is an almost a pass through, most of the work is done via partition & sort.
 *
 * The input key is a long, where the top 32 bits is an externalized feature id that we can
 * use to lookup the namespace, and the bottom 32 bits is an ivory date representation that we
 * can use to determine the partition to write out to.
 *
 * The input value is the bytes representation of the fact ready to write out.
 *
 * The output is a sequence file, with no key, and the bytes of the serialized Fact. The output
 * is partitioned by namespace and date (determined by the input key).
 */
class IngestReducer extends Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable] {
  var ctx: MrContext = null
  var out: MultipleOutputs[NullWritable, BytesWritable] = null
  var lookup: NamespaceLookup = new NamespaceLookup

  override def setup(context: Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, IngestJob.Keys.NamespaceLookup, lookup)
    out = new MultipleOutputs(context)
  }

  override def cleanup(context: Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit =
    out.close()

  override def reduce(key: LongWritable, iter: JIterable[BytesWritable], context: Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit = {
    val path = IngestJob.partitionFor(lookup, key)
    val iterator = iter.iterator
    while (iterator.hasNext)
      out.write(IngestJob.Keys.Out, NullWritable.get, iterator.next, path);
  }
}
