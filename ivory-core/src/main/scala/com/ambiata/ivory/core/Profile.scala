package com.ambiata.ivory.core

object Profile {

  def time[R](name: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    if(scala.util.Random.nextDouble < 0.01) println(name + " - Elapsed time: " + (t1 - t0) + "ns")
    result
  }
}
