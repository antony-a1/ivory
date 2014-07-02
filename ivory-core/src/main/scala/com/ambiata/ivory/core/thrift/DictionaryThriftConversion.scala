package com.ambiata.ivory.core.thrift

import com.ambiata.ivory.core._
import scala.collection.JavaConverters._
import scalaz._, Scalaz._, BijectionT._

object DictionaryThriftConversion {

  import ThriftDictionaryEncoding._

  private val encoding = bijection[Id, Id, Encoding, ThriftDictionaryEncoding]({
    case BooleanEncoding => BOOLEAN
    case IntEncoding => INT
    case LongEncoding => LONG
    case DoubleEncoding => DOUBLE
    case StringEncoding => STRING
  }, {
    case BOOLEAN => BooleanEncoding
    case INT => IntEncoding
    case LONG => LongEncoding
    case DOUBLE => DoubleEncoding
    case STRING => StringEncoding
  })

  import ThriftDictionaryType._

  private val typeBi = bijection[Id, Id, Type, ThriftDictionaryType]({
    case BinaryType => BINARY
    case CategoricalType => CATEGORICAL
    case ContinuousType => CONTINOUS
    case NumericalType => NUMERICAL
  }, {
    case BINARY => BinaryType
    case CATEGORICAL => CategoricalType
    case CONTINOUS => ContinuousType
    case NUMERICAL => NumericalType
  })

  val dictionary = bijection[Id, Id, Dictionary, ThriftDictionary](
    dict =>
      new ThriftDictionary(
        dict.meta.map {
          case (FeatureId(ns, name), FeatureMeta(enc, ty, desc, tombstoneValue)) => new ThriftDictionaryFeatureId(
            ns, name) -> new ThriftDictionaryFeatureMeta(encoding.to(enc), typeBi.to(ty), desc, tombstoneValue.asJava)
        }.asJava),

    dict =>
      Dictionary(dict.meta.asScala.toMap.map {
          case (featureId, meta) => FeatureId(featureId.ns, featureId.name) ->
            FeatureMeta(encoding.from(meta.encoding), typeBi.from(meta.`type`), meta.desc, meta.tombstoneValue.asScala.toList)
        })
  )
}
