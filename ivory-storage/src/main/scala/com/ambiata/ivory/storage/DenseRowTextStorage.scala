package com.ambiata.ivory.storage

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.joda.time.LocalDate
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.alien.hdfs._
import WireFormats._

object DenseRowTextStorageV1 {

  type Entity = String
  type Attribute = String
  type StringValue = String
  
  case class DenseRowTextStorer(path: String, dict: Dictionary, delim: Char = '|', tombstone: String = "NA") extends IvoryScoobiStorer[Fact, DList[String]] {
    lazy val features: List[(Int, FeatureId, FeatureMeta)] =
      dict.meta.toList.sortBy(_._1.toString(".")).zipWithIndex.map({ case ((f, m), i) => (i, f, m) })

    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[String] = {

      val byKey: DList[((Entity, Attribute), Iterable[Fact])] =
        dlist.by(f => (f.entity, f.featureId.toString("."))).groupByKeyWith(Groupings.sortGrouping)

      val row: DList[(Entity, List[StringValue])] = byKey.map({ case ((eid, _), fs) =>
        (eid, makeDense(fs, features, tombstone))
      })
      row.map({ case (eid, vs) => eid + delim + vs.mkString(delim.toString) }).toTextFile(path.toString)
    }

    override def storeMeta: ScoobiAction[Unit] =
      ScoobiAction.fromHdfs(Hdfs.writeWith(new Path(path, ".dictionary"), os => Streams.write(os, featuresToString(features, delim).mkString("\n"))))
  }

  /**
   * Make an Iterable of Facts dense according to a dictionary. 'tombstone' is used as a value for missing facts.
   *
   * Note: It is assumed 'facts' and 'features' are sorted by FeatureId
   */
  def makeDense(facts: Iterable[Fact], features: List[(Int, FeatureId, FeatureMeta)], tombstone: String): List[StringValue] = {
    features.foldLeft((facts, List[StringValue]()))({ case ((fs, acc), (_, fid, _)) =>
      val rest = fs.dropWhile(f => f.featureId.toString(".") < fid.toString("."))
      val value = rest.headOption.collect({
        case f if f.featureId == fid => f.value
      }).getOrElse(TombstoneValue())
      val next = if(value == TombstoneValue() || rest.isEmpty) rest else rest.tail
      (next, valueToString(value, tombstone) :: acc)
    })._2.reverse
  }

  def featuresToString(features: List[(Int, FeatureId, FeatureMeta)], delim: Char): List[String] = {
    import DictionaryTextStorage._
    features.map({ case (i, f, m) => i.toString + delim + delimitedFeatureIdString(f, delim) + delim + delimitedFeatureMetaString(m, delim) })
  }

  def valueToString(v: Value, tombstoneValue: String): String = v match {
    case BooleanValue(b)  => b.toString
    case IntValue(i)      => i.toString
    case LongValue(i)     => i.toString
    case DoubleValue(d)   => d.toString
    case StringValue(s)   => s
    case TombstoneValue() => tombstoneValue
  }
}

