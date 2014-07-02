package com.ambiata.ivory.storage.legacy

import org.specs2._
import scalaz._, Scalaz._, \&/._

import com.ambiata.ivory.core._

class DictionaryTextStorageSpec extends Specification { def is = s2"""

  Parsing a dictionary entry can:
    extract to completion when all fields are valid $e1
    catch invalid encodings                         $e2
    catch invalid types                             $e3

  Given a dictionary file we can:
    load it successfully if it is valid             $e4
    fail if it has invalid entries                  $e5
                                                    """

  def e1 = {
    val entry = "demo|postcode|string|categorical|Postcode|☠"
    DictionaryTextStorage.parseDictionaryEntry(entry) must_== ((FeatureId("demo", "postcode"), FeatureMeta(StringEncoding, CategoricalType, "Postcode"))).right
  }

  def e2 = {
    val entry = "demo|postcode|strin|categorical|Postcode|☠"
    DictionaryTextStorage.parseDictionaryEntry(entry).toEither must beLeft(contain("not a valid encoding: 'strin'"))
  }

  def e3 = {
    val entry = "demo|postcode|string|cat|Postcode|☠"
    DictionaryTextStorage.parseDictionaryEntry(entry).toEither must beLeft(contain("not a valid feature type: 'cat'"))
  }

  def e4 = {
   DictionaryTextStorage.fromFile("ivory-storage/src/test/resources/good_dictionary.txt").run.unsafePerformIO().toDisjunction must_== Dictionary(Map(
     FeatureId("demo", "gender")            -> FeatureMeta(StringEncoding, CategoricalType, "Gender"),
     FeatureId("demo", "postcode")          -> FeatureMeta(StringEncoding, CategoricalType, "Postcode"),
     FeatureId("widgets", "count.1W") -> FeatureMeta(IntEncoding, NumericalType, "Count in the last week")
   )).right
  }

  def e5 = {
    DictionaryTextStorage.fromFile("ivory-storage/src/test/resources/bad_dictionary.txt").run.unsafePerformIO().toEither must beLeft
  }
}
