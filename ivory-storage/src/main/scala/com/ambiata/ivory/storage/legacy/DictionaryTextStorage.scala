package com.ambiata.ivory.storage.legacy

import scalaz.{Value => _, _}, Scalaz._, \&/._, effect.IO
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse._

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._

object DictionaryTextStorage {

  case class DictionaryTextLoader(path: Path) extends IvoryLoader[Hdfs[Dictionary]] {
    def load: Hdfs[Dictionary] =
      Hdfs.readWith(path, fromInputStream(path.getName, _))
  }

  case class DictionaryTextStorer(path: Path, delim: Char = '|') extends IvoryStorer[Dictionary, Hdfs[Unit]] {
    def store(dict: Dictionary): Hdfs[Unit] =
      Hdfs.writeWith(path, os => Streams.write(os, delimitedDictionaryString(dict, delim)))
  }

  def dictionaryFromHdfs(path: Path): Hdfs[Dictionary] =
    DictionaryTextLoader(path).load

  def dictionaryToHdfs(path: Path, dict: Dictionary): Hdfs[Unit] =
    DictionaryTextStorer(path).store(dict)

  def fromInputStream(is: java.io.InputStream): ResultTIO[Dictionary] = for {
    content <- Streams.read(is)
    r <- ResultT.fromDisjunction[IO, Dictionary](fromLines(content.lines.toList).leftMap(This(_)))
  } yield r

  def fromString(s: String): String \/ Dictionary =
    fromLines(s.lines.toList)

  def fromLines(lines: List[String]): String \/ Dictionary = {
    val numbered = lines.zipWithIndex.map({ case (l, n) => (l, n + 1) })
    numbered.map({ case (l, n) => parseDictionaryEntry(l).leftMap(e => s"Line $n: $e")}).sequenceU.map(entries => Dictionary(entries.toMap))
  }

  def fromFile(path: String): ResultTIO[Dictionary] = {
    val file = new java.io.File(path)
    for {
      raw <- Files.read(file.getAbsolutePath.toFilePath)
      fs  <- ResultT.fromDisjunction[IO, Dictionary](fromLines(raw.lines.toList).leftMap(err => This(s"Error reading dictionary from file '$path': $err")))
    } yield fs
  }

  def writeFile(dict: Dictionary, path: String, delim: Char = '|'): ResultTIO[Unit] = ResultT.safe({
    Streams.write(new java.io.FileOutputStream(path), delimitedDictionaryString(dict, delim))
  })

  def delimitedDictionaryString(dict: Dictionary, delim: Char): String =
    dict.meta.map({ case (featureId, featureMeta) =>
      delimitedFeatureIdString(featureId, delim) + delim + delimitedFeatureMetaString(featureMeta, delim)
    }).mkString("\n") + "\n"

  def delimitedFeatureIdString(fid: FeatureId, delim: Char): String =
    fid.namespace + delim + fid.name

  def delimitedFeatureMetaString(meta: FeatureMeta, delim: Char): String =
    encodingString(meta.encoding) + delim + typeString(meta.ty) + delim + meta.desc + delim + meta.tombstoneValue.mkString(",")

  def encodingString(enc: Encoding): String = enc match {
    case BooleanEncoding    => "boolean"
    case IntEncoding        => "int"
    case LongEncoding       => "long"
    case DoubleEncoding     => "double"
    case StringEncoding     => "string"
  }

  def typeString(ty: Type): String = ty match {
    case NumericalType   => "numerical"
    case ContinuousType  => "continuous"
    case CategoricalType => "categorical"
    case BinaryType      => "binary"
  }

  def parseDictionaryEntry(entry: String): String \/ (FeatureId, FeatureMeta) = {
    import ListParser._
    val parser: ListParser[(FeatureId, FeatureMeta)] = for {
      namespace <- string
      name      <- string
      encoding  <- for {
        s <- string
        r <- value(parseEncoding(s).leftMap(_ => s"""not a valid encoding: '$s'"""))
      } yield r
      ty        <- for {
        s <- string
        r <- value(parseType(s).leftMap(_ => s"""not a valid feature type: '$s'"""))
      } yield r
      desc      <- string
      tombstone <- string
    } yield (FeatureId(namespace, name), FeatureMeta(encoding, ty, desc, Delimited.parseCsv(tombstone)))
    parser.run(Delimited.parsePsv(entry)).disjunction
  }

  def parseEncoding(s: String): Validation[String, Encoding] =
    s match {
      case "boolean"    => BooleanEncoding.success
      case "int"        => IntEncoding.success
      case "long"       => IntEncoding.success
      case "double"     => DoubleEncoding.success
      case "string"     => StringEncoding.success
      case otherwise    => "".failure
    }

  def parseType(s: String): Validation[String, Type] =
    s match {
      case "numerical"    => NumericalType.success
      case "continuous"   => ContinuousType.success
      case "categorical"  => CategoricalType.success
      case "binary"       => BinaryType.success
      case otherwise      => "".failure
    }

}
