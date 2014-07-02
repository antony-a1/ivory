package com.ambiata.ivory.cli

object ScoptReaders {

  implicit val charRead: scopt.Read[Char] =
    scopt.Read.reads(str => {
      val chars = str.toCharArray
      chars.length match {
        case 0 => throw new IllegalArgumentException(s"'${str}' can not be empty!")
        case 1 => chars(0)
        case l => throw new IllegalArgumentException(s"'${str}' is not a char!")
      }
    })
}
