package com.ambiata.ivory.mr

import com.ambiata.ivory.core.thrift._
import com.ambiata.mundane.io._

import scalaz._, Scalaz._

import org.specs2._, matcher._, specification.{Text => _, _}
import org.scalacheck._, Arbitrary._

import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util._


class ThriftCacheSpec extends Specification with ScalaCheck { def is = s2"""

ThriftCache
-----------

  Accessible from mapper                    $map

"""
  def map = {
    val conf = new Configuration
    val job = Job.getInstance(conf)
    job.setJobName("ThriftCacheSpec")
    job.setMapperClass(classOf[ThriftCacheSpecMapper])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[Text])
    Files.write("target/test/in/ThriftCacheSpec.csv".toFilePath, "fred,10\nbarney,1000\n").run.unsafePerformIO
    FileInputFormat.addInputPaths(job, "target/test/in/ThriftCacheSpec.csv")
    FileOutputFormat.setOutputPath(job, new Path("target/test/out/ThriftCacheSpec-" + java.util.UUID.randomUUID))
    val fact = new ThriftFact("entity", "test", ThriftFactValue.s("value"))
    ThriftCache.push(job, ThriftCache.Key("test"), fact)
    job.waitForCompletion(true)
  }
}

class ThriftCacheSpecMapper extends Mapper[LongWritable, Text, LongWritable, Text] {
  val value = new ThriftFact

  override def setup(context: Mapper[LongWritable, Text, LongWritable, Text]#Context): Unit = {
    ThriftCache.pop(ThriftCache.Key("test"), value)
    if (value.attribute != "test")
      sys.error("Did not deserialize ThriftFact from cache")
  }
}
