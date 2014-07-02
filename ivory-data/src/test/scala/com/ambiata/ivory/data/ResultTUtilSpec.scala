package com.ambiata.ivory.data

import com.ambiata.mundane.control._
import org.specs2._
import scalaz._, Scalaz._

class ResultTUtilSpec extends Specification { def is = s2"""

RestultT Util Spec
------------------

  Retry                                      $retry
  Retry fail                                 $retryFail

"""

  def retry = retryTest(2) must beSome(2)

  def retryFail = retryTest(6) must beNone

  private def retryTest(max: Int) = {
    var i = -1
    ResultTUtil.retry(5) {
      i = i + 1
      if (i < max) ResultT.fail[Identity, Int](s"fail $i") else ResultT.ok[Identity, Int](i)
    }.toOption.value
  }
}
