package com.ambiata.ivory.generate

import scalaz._, Scalaz._, effect._
import com.nicta.rng._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.alien.hdfs._

object GenerateDictionary {
  type Nel[A] = NonEmptyList[A]
  import Rng._

  def onHdfs(namespaces: Int, features: Int): Hdfs[Dictionary] = for {
    words <- Hdfs.fromResultTIO(readWords)
    dict  <- Hdfs.fromIO(randomDictionary(namespaces, features, words).run)
  } yield dict

  def readWords: ResultTIO[Nel[String]] = for {
    lines <- Streams.read(getClass.getResourceAsStream("/words.txt")).map(_.lines.toList)
    _     <- if (lines.length < 1) ResultT.fail[IO, Unit](s"Missing words.txt resource!") else ResultT.ok[IO, Unit](())
  } yield NonEmptyList(lines.head, lines.tail: _*)

  def randomDictionary(namespaces: Int, features: Int, words: Nel[String]): Rng[Dictionary] = for {
    nss     <- randomNamespaces(namespaces, words)
    names   <- randomNames(features, words)
    entries <- randomDictionaryEntries(nss, names)
  } yield Dictionary(entries.toMap)

  def randomDictionaryEntries(namespaces: Nel[String], names: Nel[String]): Rng[List[(FeatureId, FeatureMeta)]] =
    names.list.map(nm => randomDictionaryEntry(namespaces, nm)).sequenceU

  def randomDictionaryEntry(namespaces: Nel[String], name: String): Rng[(FeatureId, FeatureMeta)] = for {
    ns  <- oneof(namespaces.head, namespaces.tail: _*)
    fm  <- randomFeatureMeta
  } yield (FeatureId(ns, name), fm)

  def randomNamespaces(n: Int, words: Nel[String]): Rng[Nel[String]] = for {
    nss <- randomNWords(n, words.list, Rng.insert('_'))
  } yield NonEmptyList(nss.head, nss.tail: _*)

  def randomNames(n: Int, words: Nel[String]): Rng[Nel[String]] = for {
    nss <- randomNWords(n, words.list, randomSeparatorChar)
  } yield NonEmptyList(nss.head, nss.tail: _*)

  def randomNWords(n: Int, words: List[String], delim: Rng[Char]): Rng[List[String]] =
    (if(n > words.length) expandWords(n, words, delim) else Rng.insert(words)).map(l => scala.util.Random.shuffle(l).take(n))

  def expandWords(n: Int, words: List[String], delim: Rng[Char]): Rng[List[String]] =
    words.combinations(math.ceil(logbase(n, words.length)).toInt).map(l => delim.map(c => l.mkString(c.toString))).toList.sequence

  def logbase(x: Double, base: Double): Double =
    math.log(x) / math.log(base)

  def randomSeparatorChar: Rng[Char] =
    frequency((1, oneof(':', '#', '.')), (2, alphanumeric))

  def randomFeatureMeta: Rng[FeatureMeta] = for {
    ty   <- randomType
    enc  <- randomEncoding(ty)
    desc <- randomDescription
  } yield FeatureMeta(enc, ty, desc)

  def randomEncoding(ty: Type): Rng[Encoding] = ty match {
    case NumericalType   => oneof(IntEncoding, DoubleEncoding, LongEncoding)
    case ContinuousType  => oneof(IntEncoding, DoubleEncoding, LongEncoding)
    case CategoricalType => oneof(BooleanEncoding, IntEncoding, StringEncoding, LongEncoding)
    case BinaryType      => oneof(BooleanEncoding, IntEncoding, DoubleEncoding)
  }

  def randomType: Rng[Type] =
    oneof(NumericalType, ContinuousType, CategoricalType, BinaryType)

  def randomDescription: Rng[String] =
    alphanumericstring(50)
}
