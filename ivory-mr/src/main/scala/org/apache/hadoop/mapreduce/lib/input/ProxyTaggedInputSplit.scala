package org.apache.hadoop.mapreduce.lib.input

import org.apache.hadoop.mapreduce.InputSplit

/**
 * This is used to get inside the hadoop package structure because
 * TaggedInputSplit is package protected, and we need to get to the
 * underlying InputSplit
 */
case class ProxyTaggedInputSplit(split: TaggedInputSplit) {
  def getUnderlying: InputSplit =
    split.getInputSplit()
}

object ProxyTaggedInputSplit {
  def fromInputSplit(split: InputSplit) =
    ProxyTaggedInputSplit(split.asInstanceOf[TaggedInputSplit])
}
