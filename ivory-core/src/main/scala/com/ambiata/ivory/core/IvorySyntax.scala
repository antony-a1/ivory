package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import org.apache.hadoop.fs.Path

object IvorySyntax {
  implicit class IvoryFilePathSyntax(f: FilePath) {
    def toHdfs: Path = new Path(f.path)
  }
}
