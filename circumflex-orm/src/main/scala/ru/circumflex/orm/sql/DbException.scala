package ru.circumflex.orm.sql

import java.io.IOException
import java.sql.SQLException

object DbException {

  def getSqlException(message: String, errorCode: Int, cause: Throwable): SQLException = {
    val sqlstate = ErrorCode.getState(errorCode)
    new SQLException(message, sqlstate, errorCode, cause)
  }

  def getSqlException(message: String, errorCode: Int): SQLException = {
    val sqlstate = ErrorCode.getState(errorCode)
    new SQLException(message, sqlstate, errorCode)
  }

  def getUnsupportedException(): SQLException = {
    getSqlException("Feature not supported", ErrorCode.FEATURE_NOT_SUPPORTED_1)
  }

  def convertIOException(e: IOException, message: String): SQLException = {
    if (message == null) {
      e.getCause match {
        case x: SQLException => x
        case _ => getSqlException(e.toString, ErrorCode.IO_EXCEPTION_1, e)
      }
    } else {
      getSqlException(message, ErrorCode.IO_EXCEPTION_2, e)
    }
  }
  
  def convertToIOException(e: Throwable): IOException = {
    e match {
      case x: IOException => x
      case _ =>
        val io = new IOException(e.toString)
        io.initCause(e)
        io
    }
  }
}
