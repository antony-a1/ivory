package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.testing.DirIO
import com.ambiata.mundane.testing.ResultMatcher._
import org.specs2.Specification
import scalaz._, Scalaz._

class DictionaryThriftStorageSpec extends Specification { def is = s2"""

  Given a dictionary we can:
    store and then load it successfully             $e1
                                                    """

  val dict = Arbitraries.TestDictionary

  def e1 = DirIO.run { dir =>
    val loader = DictionaryThriftStorage(Repository.fromLocalPath(dir))
    loader.store(dict) >> loader.load
  } must beOkValue(dict)
}
