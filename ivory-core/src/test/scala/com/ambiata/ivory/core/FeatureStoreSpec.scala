//package com.ambiata.ivory.core
//
//import java.io.File
//import org.joda.time.LocalDate
//
//import org.specs2._
//import scalaz._, Scalaz._
//
//
//class FeatureStoreSpec extends Specification { def is = s2"""
//
//  Can extract the partition information for an EAVT file $e1
//
//                                                    """
//  def e1 = {
//    val eavt = new File("src/test/resources/feature_store/fact_set_00000/demographics/2013/11/27/xaa")
//    FeatureStore.parseEavtPartitions(eavt) must_== EavtFile(eavt, "demographics", new LocalDate(2013, 11, 27)).right
//  }
//}
