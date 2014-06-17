package org.apache.hadoop.mapreduce.lib.input;

import org.apache.hadoop.mapreduce.InputSplit;

public class ProxyTaggedInputSplit {

  private TaggedInputSplit split;

  public ProxyTaggedInputSplit(TaggedInputSplit split) {
    this.split = split;
  }

  public static ProxyTaggedInputSplit fromInputSplit(InputSplit split) {
    return new ProxyTaggedInputSplit((TaggedInputSplit) split);
  }

  public InputSplit getUnderlying() {
    return split.getInputSplit();
  }
}
