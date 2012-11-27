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
import org.apache.avro.reflect.ReflectDatumReader
import org.apache.avro.reflect.ReflectDatumWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import ru.circumflex.orm.Field
import ru.circumflex.orm.Relation
import ru.circumflex.orm.sql.DbException
import ru.circumflex.orm.sql.ErrorCode
import ru.circumflex.orm.sql.SimpleResultSet
import ru.circumflex.orm.sql.SimpleResultSet.Column
import ru.circumflex.orm.sql.SimpleRowSource


/**
 * A facility to read from and write to AVRO files
 * 
 * We have to use org.apache.avro.reflect.ReflectDatumReader/Writer, since whoes 
 * isByte() accepts Array[Byte]. (The generic one only accepts java.nio.ByteBuffer)
 *
 * @author Caoyuan Deng
 */
object Avro {
  private val nullableUnionSchemasCache = {
    val xs = new java.util.ArrayList[Schema](2)
    xs.add(null) // to be set
    xs.add(Schema.create(Schema.Type.NULL))
    xs
  }
  
  // should be declared after nullableUnionSchemasCache to get calling on makeNullable don't throws NPE
  private val THROWABLE_MESSAGE = makeNullable(Schema.create(Schema.Type.STRING))
  
  def makeNullable(schema: Schema): Schema = {
    nullableUnionSchemasCache.set(0, schema)
    Schema.createUnion(nullableUnionSchemasCache)
  }

  def schemaFieldToColumn(field: Schema.Field): Column = {
    import Schema.Type
    val sqlType = schemaTypeToSqlType(field.schema)
    field.schema.getType match {
      case Type.FIXED => Column(field.name, sqlType, field.schema.getFixedSize, 0)
      case _ => Column(field.name, sqlType, Integer.MAX_VALUE, 0)
    }
  }
  
  private def schemaTypeToSqlType(schema: Schema): Int = {
    import Schema.Type
    schema.getType match {
      case Type.UNION => schemaTypeToSqlType(schema.getTypes.get(0)) // @see makeNullable
      case Type.INT =>     Types.INTEGER
      case Type.LONG =>    Types.BIGINT
      case Type.FLOAT =>   Types.FLOAT
      case Type.DOUBLE =>  Types.DOUBLE
      case Type.FIXED =>   Types.VARBINARY
      case Type.BYTES =>   Types.VARBINARY
      case Type.STRING =>  Types.VARCHAR
      case Type.BOOLEAN => Types.BOOLEAN
      case x => throw new Exception("Unknown scheme type: " + x)
    }
  }

  def sqlTypeToSchemaType(sqlType: Int): Schema.Type = {
    import Schema.Type
    sqlType match {
      case Types.VARCHAR   => Type.STRING
      case Types.VARBINARY => Type.BYTES
      case Types.BOOLEAN   => Type.BOOLEAN
      case Types.INTEGER   => Type.INT
      case Types.BIGINT    => Type.LONG
      case Types.FLOAT  | Types.REAL    => Type.FLOAT
      case Types.DOUBLE | Types.DECIMAL => Type.DOUBLE
      case x => throw new Exception("Unknown sql type: " + x)
    }
  }

  def metaToSchema(meta: ResultSetMetaData, name: String): Schema = {
    val columnCount = meta.getColumnCount

    val schema = Schema.createRecord(name, null, "", false)
    val avroFields = new java.util.ArrayList[Schema.Field]()
    var i = -1
    while ({i += 1; i < columnCount}) {
      val columnName = meta.getColumnLabel(i + 1)
      val columnType = meta.getColumnType(i + 1)
      val schameType = sqlTypeToSchemaType(columnType)
      val fieldSchema = Avro.makeNullable(Schema.create(schameType)) // column value in ResultSet is object, just make them nullable
      val avroField = new Schema.Field(columnName, fieldSchema, null, null)
      avroFields.add(avroField)
    }
    schema.setFields(avroFields)
    schema
  }

  def apply() = new Avro

  // --- utilies for reference only
  
  private def createAvroSchema(relation: Relation[_]): Schema = {
    val recordClass = relation.recordClass
    val name = recordClass.getSimpleName
    val space = if (recordClass.getEnclosingClass != null) { // nested class
      recordClass.getEnclosingClass.getName + "$"
    } else {
      Option(recordClass.getPackage) map (_.getName) getOrElse ""
    }
    val error = classOf[Throwable].isAssignableFrom(recordClass)

    val schema = Schema.createRecord(name, null, space, error)
    val avroFields = new java.util.ArrayList[Schema.Field]()
    for (field <- relation.fields) {
      val fieldSchema = createAvroFieldSchema(field)
      val avroField = new Schema.Field(field.name, fieldSchema, null, null)
      avroFields.add(avroField)
    }
    if (error) { // add Throwable message
      avroFields.add(new Schema.Field("detailMessage", Avro.THROWABLE_MESSAGE, null, null))
    }
    schema.setFields(avroFields)

    schema
  }

  private def createAvroFieldSchema(field: Field[_, _]): Schema = {
    val fieldSchema = Schema.create(field.avroType)
    if (!field.notNull_?) { // nullable
      Avro.makeNullable(fieldSchema)
    } else {
      fieldSchema
    }
  }
}

import Avro._
class Avro private () extends SimpleRowSource {

  private var columnNames: Array[String] = _
  private var fileName: String = _

  private var reader: DataFileReader[AnyRef] = _
  private var writer: DataFileWriter[AnyRef] = _
  private var record: GenericRecord = _ // reusable record

  private var schemaFields = Map[String, Schema.Field]()

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
  def write(outputFileName: String, rs: ResultSet, tableName: String): Int = {
    init(outputFileName)
    try {
      initWrite()
      writeResultSet(rs, tableName)
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
  def write(conn: Connection, outputFileName: String, sql: String, tableName: String): Int = {
    val stat = conn.createStatement
    val rs = stat.executeQuery(sql)
    val nRows = write(outputFileName, rs, tableName)
    stat.close
    nRows
  }

  @throws(classOf[SQLException])
  private def writeResultSet(rs: ResultSet, tableName: String): Int =  {
    try {
      val file = new File(fileName)
      val meta = rs.getMetaData
      val avroSchema = metaToSchema(meta, tableName)
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
        case _: Throwable =>
      }
    }
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
  def read(inputFileName: String, colNames: Array[String]): ResultSet = {
    init(inputFileName)
    try {
      readResultSet(colNames)
    } catch {
      case e: IOException => throw convertException("IOException reading " + inputFileName, e)
    }
  }

  @throws(classOf[IOException])
  private def readResultSet(colNames: Array[String]): ResultSet = {
    this.columnNames = colNames
    initRead

    val result = new SimpleResultSet(this)
    for (columnName <- columnNames) {
      import Schema.Type
      val column = schemaFields.get(columnName) match {
        case Some(schemaField) => Avro.schemaFieldToColumn(schemaField)
        case _ => Column(columnName, Types.VARCHAR, Integer.MAX_VALUE, 0) // todo
      }
      result.addColumn(column)
    }
    result
  }

  private def init(newFileName: String) {
    this.fileName = newFileName
  }

  @throws(classOf[IOException])
  private def initWrite() {
    if (writer == null) {
      try {
        writer = new DataFileWriter[AnyRef](new ReflectDatumWriter[AnyRef]())//.setSyncInterval(syncInterval)
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
        reader = new DataFileReader[AnyRef](new File(fileName), new ReflectDatumReader[AnyRef]())
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
      case _ =>
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
          row(i) = record.get(colName) match {
            case x: Utf8 => x.toString
            case x => x
          }
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
        case _: Throwable => 
      }
    reader = null

    if (writer != null)
      try {
        writer.close
      } catch {
        case _: Throwable =>
      }
    writer = null
  }

  @throws(classOf[SQLException])
  def reset() {
    throw new SQLException("Method is not supported", "AVRO")
  }

}
