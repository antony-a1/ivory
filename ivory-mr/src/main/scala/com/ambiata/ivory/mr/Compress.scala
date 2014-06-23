package com.ambiata.ivory.mr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

object Compress {

  def intermediate(job: Job, codec: CompressionCodec): Unit = {
    // MR1
    job.getConfiguration.set("mapred.compress.map.output", "true")
    job.getConfiguration.set("mapred.map.output.compression.codec", codec.getClass.getName)

    // YARN
    job.getConfiguration.set("mapreduce.map.output.compress", "true")
    job.getConfiguration.set("mapred.map.output.compress.codec", codec.getClass.getName)
  }

  def output(job: Job, codec: CompressionCodec): Unit = {
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK)
    FileOutputFormat.setCompressOutput(job, true)
    FileOutputFormat.setOutputCompressorClass(job, codec.getClass)
  }
}
