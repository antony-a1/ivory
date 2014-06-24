package org.apache.hadoop.mapreduce.lib.input

import org.apache.hadoop.mapreduce.InputSplit

/**
 * This is used to get inside the hadoop package structure because
 * TaggedInputSplit is package protected, and we need to get to the
 * underlying InputSplit
 */
sealed trait ProxiedInputSplit {
  def getUnderlying: InputSplit
}
case class ProxiedTaggedInputSplit(split: TaggedInputSplit) extends ProxiedInputSplit {
  def getUnderlying: InputSplit =
    split.getInputSplit()
}
case class ProxiedGenericInputSplit(split: InputSplit) extends ProxiedInputSplit {
  def getUnderlying: InputSplit =
    split
}

object ProxiedInputSplit {
  def fromInputSplit(split: InputSplit) = split match {
    case tagged: TaggedInputSplit => ProxiedTaggedInputSplit(tagged)
    case other                    => ProxiedGenericInputSplit(other)
  }
}
