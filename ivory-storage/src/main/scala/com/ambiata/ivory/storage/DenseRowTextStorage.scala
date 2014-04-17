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

object DenseRowTextStorage {

  type Entity = String
  type Attribute = String
  type StringValue = String
  
  case class DenseRowTextStorer(path: String, dict: Dictionary, delim: Char = '|', tombstoneValue: String = "NA") extends IvoryScoobiStorer[Fact, DList[String]] {
    lazy val features: List[(Int, FeatureId, FeatureMeta)] =
      dict.meta.toList.sortBy(_._1.toString(".")).zipWithIndex.map({ case ((f, m), i) => (i, f, m) })

    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[String] = {

      val byKey: DList[((Entity, Attribute), Iterable[Fact])] =
        dlist.by(f => (f.entity, f.featureId.toString("."))).groupByKeyWith(Groupings.sortGrouping)

      val row: DList[(Entity, List[StringValue])] = byKey.map({ case ((eid, _), fs) =>
        (eid, features.foldLeft((fs, List[StringValue]()))({ case ((facts, acc), (_, fid, _)) =>
          val rest = facts.dropWhile(f => f.featureId < fid)
          val v = rest.headOption.collect({
            case f if f.featureId == fid => f.value
          }).getOrElse(TombstoneValue())
          (if(rest.isEmpty) Nil else rest.tail, valueToString(v, tombstoneValue) :: acc)
        })._2)
      })
      row.map({ case (eid, vs) => eid + delim + vs.mkString(delim.toString) }).toTextFile(path.toString)
    }

    override def storeMeta: ScoobiAction[Unit] =
      ScoobiAction.fromHdfs(Hdfs.writeWith(new Path(path, ".dictionary"), os => Streams.write(os, featuresToString(features, delim).mkString("\n"))))
  }

  def featuresToString(features: List[(Int, FeatureId, FeatureMeta)], delim: Char): List[String] = {
    import DictionaryTextStorage._
    features.map({ case (i, f, m) => i + delim + delimitedFeatureIdString(f, delim) + delim + delimitedFeatureMetaString(m, delim) })
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

