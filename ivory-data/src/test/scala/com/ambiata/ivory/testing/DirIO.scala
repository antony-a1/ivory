package com.ambiata.ivory.testing

import com.ambiata.mundane, mundane.control._, mundane.io.FilePath, mundane.testing._

// TODO Mundane?!?
object DirIO {

  def run[A](f: FilePath => ResultTIO[A]): Result[A] = {
    val dir = Dirs.mkTempDir("deleteme")
    try f(FilePath(dir.getAbsolutePath)).run.unsafePerformIO()
    finally Dirs.rmdir(dir)
  }
}
