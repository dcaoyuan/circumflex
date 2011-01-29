package ru.circumflex.orm.avro


import java.io.File
import java.io.IOException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Types
import java.util.ArrayList

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import ru.circumflex.orm.sql.DbException
import ru.circumflex.orm.sql.ErrorCode
import ru.circumflex.orm.sql.SimpleResultSet
import ru.circumflex.orm.sql.SimpleResultSet.Column
import ru.circumflex.orm.sql.SimpleRowSource


/**
 * A facility to read from and write to AVRO files
 *
 * @author Caoyuan Deng
 */
object Avro {
  val THROWABLE_MESSAGE = makeNullable(Schema.create(Schema.Type.STRING))

  def makeNullable(schema: Schema): Schema = {
    Schema.createUnion(java.util.Arrays.asList(Schema.create(Schema.Type.NULL), schema))
  }

  def schemFieldToColumn(field: Schema.Field): Column = {
    import Schema.Type
    field.schema.getType match {
      case Type.BOOLEAN => Column(field.name, Types.BOOLEAN,   Integer.MAX_VALUE, 0)
      case Type.BYTES =>   Column(field.name, Types.VARBINARY, Integer.MAX_VALUE, 0)
      case Type.DOUBLE =>  Column(field.name, Types.DOUBLE,    Integer.MAX_VALUE, 0)
      case Type.FIXED =>   Column(field.name, Types.VARBINARY, field.schema.getFixedSize, 0)
      case Type.FLOAT =>   Column(field.name, Types.FLOAT,     Integer.MAX_VALUE, 0)
      case Type.INT =>     Column(field.name, Types.INTEGER,   Integer.MAX_VALUE, 0)
      case Type.LONG =>    Column(field.name, Types.BIGINT,    Integer.MAX_VALUE, 0)
      case Type.STRING =>  Column(field.name, Types.VARCHAR,   Integer.MAX_VALUE, 0)
    }
  }

  def sqlTypeToSchemaType(sqlType: Int): Schema.Type = {
    import Schema.Type
    sqlType match {
      case Types.BOOLEAN => Type.BOOLEAN
      case Types.VARBINARY => Type.BYTES
      case Types.DOUBLE | Types.DECIMAL => Type.DOUBLE
      case Types.FLOAT => Type.FLOAT
      case Types.INTEGER => Type.INT
      case Types.BIGINT => Type.LONG
      case Types.VARCHAR => Type.STRING
    }
  }

  def metaToSchema(meta: ResultSetMetaData): Schema = {
    val columnCount = meta.getColumnCount

    val schema = Schema.createRecord("anonymous", null, "", false)
    val avroFields = new java.util.ArrayList[Schema.Field]()
    var i = 0
    while (i < columnCount) {
      val columnName = meta.getColumnLabel(i + 1)
      val columnType = meta.getColumnType(i + 1)
      val schameType = sqlTypeToSchemaType(columnType)
      val fieldSchema = Schema.create(schameType)
      val avroField = new Schema.Field(columnName, fieldSchema, null, null)
      avroFields.add(avroField)

      i += 1
    }
    schema.setFields(avroFields)
    schema
  }

  def apply() = new Avro
}

import Avro._
class Avro private () extends SimpleRowSource {

  private var columnNames: Array[String] = _
  private var fileName: String = _

  private var reader: DataFileReader[AnyRef] = _
  private var writer: DataFileWriter[AnyRef] = _
  private var record: GenericRecord = _ // reusable record


  private var schemaFields = Map[String, Schema.Field]()


  @throws(classOf[SQLException])
  private def writeResultSet(rs: ResultSet): Int =  {
    try {
      val file = new File(fileName)
      val meta = rs.getMetaData
      val avroSchema = metaToSchema(meta)
      writer.create(avroSchema, file)

      val columnCount = meta.getColumnCount
      val record = new GenericData.Record(avroSchema)
      val row = new Array[AnyRef](columnCount)
      var nRows = 0
      while (rs.next) {
        var i = 0
        while (i < columnCount) {
          val name = meta.getColumnLabel(i + 1)
          val value = rs.getObject(i + 1)
          record.put(name, value)
          i += 1
        }
        writeRow(record)
        nRows += 1
      }
      nRows
    } catch {
      case e: IOException => throw DbException.convertIOException(e, null);
    } finally {
      try {
        close
        rs.close
      } catch {
        case _ =>
      }
    }
  }

  /**
   * Writes the result set to a file in the CSV format.
   *
   * @param writer the writer
   * @param rs the result set
   * @return the number of rows written
   * @throws SQLException
   */
  @throws(classOf[SQLException])
  def write(writer: DataFileWriter[AnyRef], rs: ResultSet): Int = {
    this.writer = writer
    writeResultSet(rs)
  }

  /**
   * Writes the result set to a file in the CSV format. The result set is read
   * using the following loop:
   *
   * <pre>
   * while (rs.next()) {
   *     writeRow(row);
   * }
   * </pre>
   *
   * @param outputFileName the name of the csv file
   * @param rs the result set - the result set must be positioned before the
   *          first row.
   * @param charset the charset or null to use the system default charset
   *          (see system property file.encoding)
   * @return the number of rows written
   * @throws SQLException
   */
  @throws(classOf[SQLException])
  def write(outputFileName: String, rs: ResultSet, charset: String): Int = {
    init(outputFileName, charset)
    try {
      initWrite(rs.getMetaData)
      writeResultSet(rs)
    } catch {
      case e: IOException => throw convertException("IOException writing " + outputFileName, e);
    }
  }

  /**
   * Writes the result set of a query to a file in the CSV format.
   *
   * @param conn the connection
   * @param outputFileName the file name
   * @param sql the query
   * @param charset the charset or null to use the system default charset
   *          (see system property file.encoding)
   * @return the number of rows written
   * @throws SQLException
   */
  @throws(classOf[SQLException])
  def write(conn: Connection, outputFileName: String, sql: String, charset: String): Int = {
    val stat = conn.createStatement
    val rs = stat.executeQuery(sql)
    val nRows = write(outputFileName, rs, charset)
    stat.close
    nRows
  }

  /**
   * Reads from the CSV file and returns a result set. The rows in the result
   * set are created on demand, that means the file is kept open until all
   * rows are read or the result set is closed.
   * <br />
   * If the columns are read from the CSV file, then the following rules are
   * used: columns names that start with a letter or '_', and only
   * contain letters, '_', and digits, are considered case insensitive
   * and are converted to uppercase. Other column names are considered
   * case sensitive (that means they need to be quoted when accessed).
   *
   * @param inputFileName the file name
   * @param colNames or null if the column names should be read from the CSV
   *          file
   * @param charset the charset or null to use the system default charset
   *          (see system property file.encoding)
   * @return the result set
   * @throws SQLException
   */
  @throws(classOf[SQLException])
  def read(inputFileName: String, colNames: Array[String], charset: String): ResultSet = {
    init(inputFileName, charset)
    try {
      readResultSet(colNames)
    } catch {
      case e: IOException => throw convertException("IOException reading " + inputFileName, e)
    }
  }

  /**
   * Reads CSV data from a reader and returns a result set. The rows in the
   * result set are created on demand, that means the reader is kept open
   * until all rows are read or the result set is closed.
   *
   * @param reader the reader
   * @param colNames or null if the column names should be read from the CSV file
   * @return the result set
   * @throws SQLException, IOException
   */
  @throws(classOf[IOException])
  def read(reader: DataFileReader[AnyRef], colNames: Array[String]): ResultSet = {
    init(null, null)
    this.reader = reader
    readResultSet(colNames)
  }

  @throws(classOf[IOException])
  private def readResultSet(colNames: Array[String]): ResultSet = {
    this.columnNames = colNames
    initRead

    val result = new SimpleResultSet(this)
    for (columnName <- columnNames) {
      import Schema.Type
      val column = schemaFields.get(columnName) match {
        case Some(schemaField) => Avro.schemFieldToColumn(schemaField)
        case _ => Column(columnName, Types.VARCHAR, Integer.MAX_VALUE, 0) // todo
      }
      result.addColumn(column)
    }
    result
  }

  private def init(newFileName: String, charset: String) {
    this.fileName = newFileName
  }

  @throws(classOf[IOException])
  private def initWrite(meta: ResultSetMetaData) {
    if (writer == null) {
      try {
        writer = new DataFileWriter[AnyRef](AvroDatumWriter[AnyRef]())//.setSyncInterval(syncInterval)
      } catch {
        case e: Exception => close; throw DbException.convertToIOException(e)
      }
    }
  }

  @throws(classOf[IOException])
  private def writeRow(record: GenericRecord) {
    writer.append(record)
  }


  @throws(classOf[IOException])
  private def initRead() {
    if (reader == null) {
      try {
        reader = new DataFileReader[AnyRef](new File(fileName), AvroDatumReader[AnyRef]())
        readSchemaFields
      } catch {
        case e: IOException => close; throw e
      }
    }
    if (columnNames == null) {
      readColumnNames
    }
  }

  private def readSchemaFields() {
    val schema = reader.getSchema
    schema.getType match {
      case Schema.Type.RECORD =>
        val fields = schema.getFields.iterator
        while (fields.hasNext) {
          val field = fields.next
          schemaFields += (field.name -> field)
        }
    }
  }

  @throws(classOf[IOException])
  private def readColumnNames() {
    columnNames = schemaFields.keys.toArray
  }

  @throws(classOf[SQLException])
  def readRow(): Array[Any] = {
    if (reader == null) {
      return null
    }
    if (reader.hasNext) {
      val row = new Array[Any](columnNames.length)
      try {
        record = reader.next(record).asInstanceOf[GenericRecord]
        var i = 0
        while (i < columnNames.length) {
          val colName = columnNames(i)
          row(i) = record.get(colName)
          i += 1
        }
      } catch {
        case e: IOException => throw convertException("IOException reading from " + fileName, e)
      }
      row
    } else {
      null
    }
  }

  private def convertException(message: String, e: Exception): SQLException = {
    DbException.getSqlException(message, ErrorCode.IO_EXCEPTION_1, e)
  }

  def close() {
    if (reader != null) 
      try {
        reader.close
      } catch {
        case _ => 
      }
    reader = null

    if (writer != null)
      try {
        writer.close
      } catch {
        case _ =>
      }
    writer = null
  }

  @throws(classOf[SQLException])
  def reset() {
    throw new SQLException("Method is not supported", "AVRO")
  }

}
