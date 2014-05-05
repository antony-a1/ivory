package com.ambiata.ivory.core

import com.ambiata.ivory.core.thrift.ThriftParseError

/**
 * This class represents a parsing error, with the parsed line and the error message
 * An instance of this class can be serialised as a thrift record
 */
case class ParseError(line: String, message: String) {
  def toThrift = new ThriftParseError(line, message)
  def appendToMessage(msg: String) = copy(message = message + msg)
}


object ParseError {
  def withLine(line: String) = (msg: String) => ParseError(line, msg)
}
