package ru.circumflex.orm.sql

import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.Blob
import java.sql.Clob
import java.sql.SQLException
import java.sql.Date
import java.sql.Ref
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.SQLWarning
import java.sql.Statement
import java.sql.Time
import java.sql.Timestamp
import java.sql.Types
import java.util.ArrayList
import java.util.Calendar
import java.util.Map

//## Java 1.6 begin ##
import java.sql.NClob
import java.sql.RowId
import java.sql.SQLXML
//## Java 1.6 end ##

/**
 * This class is a simple result set and meta data implementation.
 * It can be used in Java functions that return a result set.
 * Only the most basic methods are implemented, the others throw an exception.
 * This implementation is standalone, and only relies on standard classes.
 * It can be extended easily if required.
 *
 * An application can create a result set using the following code:
 *
 * <pre>
 * SimpleResultSet rs = new SimpleResultSet();
 * rs.addColumn(&quot;ID&quot;, Types.INTEGER, 10, 0);
 * rs.addColumn(&quot;NAME&quot;, Types.VARCHAR, 255, 0);
 * rs.addRow(0, &quot;Hello&quot; });
 * rs.addRow(1, &quot;World&quot; });
 * </pre>
 *
 */
object SimpleResultSet {

  /**
   * This class holds the data of a result column.
   */
  case class Column (name: String, sqlType: Int, precision: Int, scale: Int)

  /**
   * A simple array implementation,
   * backed by an object array
   */
  class SimpleArray(value: Array[AnyRef]) extends java.sql.Array {

    /**
     * Get the object array.
     *
     * @return the object array
     */
    def getArray: AnyRef = {
      value
    }

    /**
     * INTERNAL
     */
    def getArray(map: java.util.Map[String, Class[_]]): AnyRef = {
      throw DbException.getUnsupportedException
    }

    /**
     * INTERNAL
     */
    @throws(classOf[SQLException])
    def getArray(index: Long, count: Int): AnyRef = {
      throw DbException.getUnsupportedException
    }

    /**
     * INTERNAL
     */
    @throws(classOf[SQLException])
    def getArray(index: Long, count: Int, map: java.util.Map[String, Class[_]]): AnyRef = {
      throw DbException.getUnsupportedException
    }

    /**
     * Get the base type of this array.
     *
     * @return Types.NULL
     */
    def getBaseType: Int = {
      Types.NULL
    }

    /**
     * Get the base type name of this array.
     *
     * @return "NULL"
     */
    def getBaseTypeName: String = {
      "NULL"
    }

    /**
     * INTERNAL
     */
    @throws(classOf[SQLException])
    def getResultSet: ResultSet = {
      throw DbException.getUnsupportedException
    }

    /**
     * INTERNAL
     */
    @throws(classOf[SQLException])
    def getResultSet(map: Map[String, Class[_]]): ResultSet = {
      throw DbException.getUnsupportedException
    }

    /**
     * INTERNAL
     */
    @throws(classOf[SQLException])
    def getResultSet(index: Long, count: Int): ResultSet = {
      throw DbException.getUnsupportedException
    }

    /**
     * INTERNAL
     */
    @throws(classOf[SQLException])
    def getResultSet(index: Long, count: Int, map: Map[String, Class[_]]): ResultSet = {
      throw DbException.getUnsupportedException
    }

    /**
     * INTERNAL
     */
    def free {
      // nothing to do
    }

  }

}

/**
 * This constructor is used if the result set should retrieve the rows using
 * the specified row source object.
 *
 * @param source the row source
 */
import SimpleResultSet._
class SimpleResultSet(private var source: SimpleRowSource) extends ResultSet with ResultSetMetaData {

  /**
   * This constructor is used if the result set is later populated with addRow.
   */
  def this() = this(null)

  private var _rows: java.util.ArrayList[Array[Any]] = if (source == null) new java.util.ArrayList[Array[Any]](4) else null
  private var _currentRow: Array[Any] = _
  private var _rowId: Int = -1
  private var _wasNull: Boolean = _
  private var _columns: java.util.ArrayList[Column] = new java.util.ArrayList[Column](4)
  private var _autoClose: Boolean = true


  /**
   * Adds a column to the result set.
   * All columns must be added before adding rows.
   *
   * @param name null is replaced with C1, C2,...
   * @param sqlType the value returned in getColumnType(..) (ignored internally)
   * @param precision the precision
   * @param scale the scale
   */
  def addColumn($name: String, sqlType: Int, precision: Int, scale: Int) {
    val name = if ($name == null) {
      "C" + (_columns.size + 1)
    } else $name
    
    addColumn(Column(name, sqlType, precision, scale))
  }

  def addColumn(column: Column) {
    if (_rows != null && _rows.size() > 0) {
      throw new IllegalStateException("Cannot add a column after adding rows");
    }
    _columns.add(column)
  }

  /**
   * Add a new row to the result set.
   * Do not use this method when using a RowSource.
   *
   * @param row the row as an array of objects
   */
  def addRow(row: Any*) {
    if (_rows == null) {
      throw new IllegalStateException("Cannot add a row when using RowSource");
    }
    _rows.add(row.toArray)
  }

  /**
   * Returns ResultSet.CONCUR_READ_ONLY.
   *
   * @return CONCUR_READ_ONLY
   */
  def getConcurrency: Int = {
    ResultSet.CONCUR_READ_ONLY
  }

  /**
   * Returns ResultSet.FETCH_FORWARD.
   *
   * @return FETCH_FORWARD
   */
  def getFetchDirection: Int = {
    ResultSet.FETCH_FORWARD
  }

  /**
   * Returns 0.
   *
   * @return 0
   */
  def getFetchSize: Int = 0

  /**
   * Returns the row number (1, 2,...) or 0 for no row.
   *
   * @return 0
   */
  def getRow: Int = {
    _rowId + 1
  }

  /**
   * Returns ResultSet.TYPE_FORWARD_ONLY.
   *
   * @return TYPE_FORWARD_ONLY
   */
  def getType: Int = {
    ResultSet.TYPE_FORWARD_ONLY
  }

  /**
   * Closes the result set and releases the resources.
   */
  def close() {
    _currentRow = null
    _rows = null
    _columns = null
    _rowId = -1
    if (source != null) {
      source.close
      source = null
    }
  }

  /**
   * Moves the cursor to the next row of the result set.
   *
   * @return true if successful, false if there are no more rows
   */
  @throws(classOf[SQLException])
  def next(): Boolean = {
    if (source != null) {
      _rowId += 1
      _currentRow = source.readRow
      if (_currentRow != null) {
        return true
      }
    } else if (_rows != null && _rowId < _rows.size) {
      _rowId += 1
      if (_rowId < _rows.size) {
        _currentRow = _rows.get(_rowId)
        return true
      }
    }
    if (_autoClose) {
      close
    }
    false
  }

  /**
   * Moves the current position to before the first row, that means resets the
   * result set.
   */
  @throws(classOf[SQLException])
  def beforeFirst() {
    _rowId = -1
    if (source != null) {
      source.reset
    }
  }

  /**
   * Returns whether the last column accessed was null.
   *
   * @return true if the last column accessed was null
   */
  def wasNull: Boolean =  {
    _wasNull
  }

  /**
   * Returns the value as a byte.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getByte(columnIndex: Int): Byte = {
    get(columnIndex) match {
      case null => 0
      case x: Number => x.byteValue
      case x: Object => java.lang.Byte.decode(x.toString).byteValue
    }
  }

  /**
   * Returns the value as an double.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getDouble(columnIndex: Int): Double = {
    get(columnIndex) match {
      case null => 0
      case x: Number => x.doubleValue
      case x: Object => java.lang.Double.parseDouble(x.toString)
    }
  }

  /**
   * Returns the value as a float.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getFloat(columnIndex: Int): Float = {
    get(columnIndex) match {
      case null => 0
      case x: Number => x.floatValue
      case x: Object => java.lang.Float.parseFloat(x.toString)
    }
  }

  /**
   * Returns the value as an int.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getInt(columnIndex: Int): Int = {
    get(columnIndex) match {
      case null => 0
      case x: Number => x.intValue
      case x: Object => java.lang.Integer.decode(x.toString).intValue
    }
  }

  /**
   * Returns the value as a long.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getLong(columnIndex: Int): Long = {
    get(columnIndex) match {
      case null => 0
      case x: Number => x.longValue
      case x: Object => java.lang.Long.decode(x.toString).longValue
    }
  }

  /**
   * Returns the value as a short.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getShort(columnIndex: Int): Short = {
    get(columnIndex) match {
      case null => 0
      case x: Number => x.shortValue
      case x: Object => java.lang.Short.decode(x.toString).shortValue
    }
  }

  /**
   * Returns the value as a boolean.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getBoolean(columnIndex: Int): Boolean = {
    get(columnIndex) match {
      case null => false
      case x: java.lang.Boolean => x.booleanValue
      case x: Object => java.lang.Boolean.valueOf(x.toString).booleanValue
    }
  }

  /**
   * Returns the value as a byte array.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getBytes(columnIndex: Int): Array[Byte] = {
    get(columnIndex).asInstanceOf[Array[Byte]]
  }

  /**
   * Returns the value as an Object.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getObject(columnIndex: Int): AnyRef = {
    get(columnIndex).asInstanceOf[AnyRef]
  }

  /**
   * Returns the value as an Object of type T.
   *
   * @param columnIndex (1,2,...)
   * @param tpe
   * @return the value
   */
  @throws(classOf[SQLException])
  def getObject[T](columnIndex: Int, tpe: Class[T]): T = {
    get(columnIndex).asInstanceOf[T]
  }

  /**
   * Returns the value as a String.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getString(columnIndex: Int): String = {
    get(columnIndex) match {
      case null => null
      case x: Object => x.toString
    }
  }

  /**
   * Returns the value as a byte.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getByte(columnLabel: String): Byte = {
    getByte(findColumn(columnLabel))
  }

  /**
   * Returns the value as a double.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getDouble(columnLabel: String): Double = {
    getDouble(findColumn(columnLabel))
  }

  /**
   * Returns the value as a float.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getFloat(columnLabel: String): Float = {
    getFloat(findColumn(columnLabel))
  }

  /**
   * Searches for a specific column in the result set. A case-insensitive
   * search is made.
   *
   * @param columnLabel the column label
   * @return the column index (1,2,...)
   * @throws SQLException if the column is not found or if the result set is
   *             closed
   */
  @throws(classOf[SQLException])
  def findColumn(columnLabel: String): Int = {
    if (columnLabel != null && _columns != null) {
      var i = 0
      while (i < _columns.size) {
        if (columnLabel.equalsIgnoreCase(getColumn(i).name)) {
          return i + 1
        }
        i += 1
      }
    }
    throw new SQLException("COLUMN_NOT_FOUND_1: " + columnLabel, ErrorCode.getState(ErrorCode.COLUMN_NOT_FOUND_1), ErrorCode.COLUMN_NOT_FOUND_1)
  }

  /**
   * Returns the value as an int.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getInt(columnLabel: String): Int = {
    getInt(findColumn(columnLabel))
  }

  /**
   * Returns the value as a long.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getLong(columnLabel: String): Long = {
    getLong(findColumn(columnLabel))
  }

  /**
   * Returns the value as a short.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getShort(columnLabel: String): Short = {
    getShort(findColumn(columnLabel))
  }

  /**
   * Returns the value as a boolean.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getBoolean(columnLabel: String): Boolean = {
    getBoolean(findColumn(columnLabel))
  }

  /**
   * Returns the value as a byte array.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getBytes(columnLabel: String): Array[Byte] = {
    getBytes(findColumn(columnLabel))
  }

  /**
   * Returns the value as a java.math.BigDecimal.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getBigDecimal(columnIndex: Int): BigDecimal = {
    get(columnIndex) match {
      case null => new BigDecimal(0)
      case x: BigDecimal => x
      case x: Object => new BigDecimal(x.toString)
    }
  }

  /**
   * Returns the value as an java.sql.Date.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getDate(columnIndex: Int): Date = {
    get(columnIndex).asInstanceOf[Date]
  }

  /**
   * Returns a reference to itself.
   *
   * @return this
   */
  def getMetaData: ResultSetMetaData = {
    this
  }

  /**
   * Returns null.
   *
   * @return null
   */
  def getWarnings: SQLWarning = {
    null
  }

  /**
   * Returns null.
   *
   * @return null
   */
  def getStatement: Statement = {
    null
  }

  /**
   * Returns the value as an java.sql.Time.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getTime(columnIndex: Int): Time = {
    get(columnIndex).asInstanceOf[Time]
  }

  /**
   * Returns the value as an java.sql.Timestamp.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getTimestamp(columnIndex: Int): Timestamp = {
    get(columnIndex).asInstanceOf[Timestamp]
  }

  /**
   * Returns the value as a java.sql.Array.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @throws(classOf[SQLException])
  def getArray(columnIndex: Int): java.sql.Array = {
    new SimpleArray(get(columnIndex).asInstanceOf[Array[AnyRef]])
  }

  /**
   * Returns the value as an Object.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getObject(columnLabel: String): AnyRef = {
    getObject(findColumn(columnLabel))
  }

  /**
   * Returns the value as an Object of type tpe.
   *
   * @param columnLabel the column label
   * @param tpe
   * @return the value
   */
  @throws(classOf[SQLException])
  def getObject[T](columnLabel: String, tpe: Class[T]): T = {
    getObject(findColumn(columnLabel), tpe)
  }

  /**
   * Returns the value as a String.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getString(columnLabel: String): String = {
    getString(findColumn(columnLabel))
  }

  /**
   * Returns the value as a java.math.BigDecimal.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getBigDecimal(columnLabel: String): BigDecimal = {
    getBigDecimal(findColumn(columnLabel))
  }

  /**
   * Returns the value as a java.sql.Date.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getDate(columnLabel: String): Date = {
    getDate(findColumn(columnLabel))
  }

  /**
   * Returns the value as a java.sql.Time.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getTime(columnLabel: String): Time = {
    getTime(findColumn(columnLabel))
  }

  /**
   * Returns the value as a java.sql.Timestamp.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getTimestamp(columnLabel: String): Timestamp = {
    getTimestamp(findColumn(columnLabel))
  }

  /**
   * Returns the value as a java.sql.Array.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @throws(classOf[SQLException])
  def getArray(columnLabel: String): java.sql.Array = {
    getArray(findColumn(columnLabel))
  }


  // ---- result set meta data ---------------------------------------------

  /**
   * Returns the column count.
   *
   * @return the column count
   */
  def getColumnCount(): Int = {
    _columns.size
  }

  /**
   * Returns 15.
   *
   * @param columnIndex (1,2,...)
   * @return 15
   */
  def getColumnDisplaySize(columnIndex: Int): Int = {
    15
  }

  /**
   * Returns the SQL type.
   *
   * @param columnIndex (1,2,...)
   * @return the SQL type
   */
  @throws(classOf[SQLException])
  def getColumnType(columnIndex: Int): Int = {
    getColumn(columnIndex - 1).sqlType
  }

  /**
   * Returns the precision.
   *
   * @param columnIndex (1,2,...)
   * @return the precision
   */
  @throws(classOf[SQLException])
  def getPrecision(columnIndex: Int): Int =  {
    getColumn(columnIndex - 1).precision
  }

  /**
   * Returns the scale.
   *
   * @param columnIndex (1,2,...)
   * @return the scale
   */
  @throws(classOf[SQLException])
  def getScale(columnIndex: Int): Int = {
    getColumn(columnIndex - 1).scale
  }

  /**
   * Returns ResultSetMetaData.columnNullableUnknown.
   *
   * @param columnIndex (1,2,...)
   * @return columnNullableUnknown
   */
  def isNullable(columnIndex: Int): Int = {
    ResultSetMetaData.columnNullableUnknown
  }

  /**
   * Returns false.
   *
   * @param columnIndex (1,2,...)
   * @return false
   */
  def isAutoIncrement(columnIndex: Int): Boolean = {
    false
  }

  /**
   * Returns true.
   *
   * @param columnIndex (1,2,...)
   * @return true
   */
  def isCaseSensitive(columnIndex: Int): Boolean = {
    true
  }

  /**
   * Returns false.
   *
   * @param columnIndex (1,2,...)
   * @return false
   */
  def isCurrency(columnIndex: Int): Boolean = {
    false
  }

  /**
   * Returns false.
   *
   * @param columnIndex (1,2,...)
   * @return false
   */
  def isDefinitelyWritable(columnIndex: Int): Boolean = {
    false
  }

  /**
   * Returns true.
   *
   * @param columnIndex (1,2,...)
   * @return true
   */
  def isReadOnly(columnIndex: Int): Boolean = {
    true
  }

  /**
   * Returns true.
   *
   * @param columnIndex (1,2,...)
   * @return true
   */
  def isSearchable(columnIndex: Int): Boolean = {
    true
  }

  /**
   * Returns true.
   *
   * @param columnIndex (1,2,...)
   * @return true
   */
  def isSigned(columnIndex: Int): Boolean = {
    true
  }

  /**
   * Returns false.
   *
   * @param columnIndex (1,2,...)
   * @return false
   */
  def isWritable(columnIndex: Int): Boolean = {
    false
  }

  /**
   * Returns null.
   *
   * @param columnIndex (1,2,...)
   * @return null
   */
  def getCatalogName(columnIndex: Int): String = {
    null
  }

  /**
   * Returns null.
   *
   * @param columnIndex (1,2,...)
   * @return null
   */
  def getColumnClassName(columnIndex: Int): String = {
    null
  }

  /**
   * Returns the column label.
   *
   * @param columnIndex (1,2,...)
   * @return the column label
   */
  @throws(classOf[SQLException])
  def getColumnLabel(columnIndex: Int): String = {
    getColumn(columnIndex - 1).name
  }

  /**
   * Returns the column name.
   *
   * @param columnIndex (1,2,...)
   * @return the column name
   */
  def getColumnName(columnIndex: Int): String = {
    getColumnLabel(columnIndex)
  }

  /**
   * Returns null.
   *
   * @param columnIndex (1,2,...)
   * @return null
   */
  def getColumnTypeName(columnIndex: Int): String = {
    null
  }

  /**
   * Returns null.
   *
   * @param columnIndex (1,2,...)
   * @return null
   */
  def getSchemaName(columnIndex: Int): String = {
    null
  }

  /**
   * Returns null.
   *
   * @param columnIndex (1,2,...)
   * @return null
   */
  def getTableName(columnIndex: Int): String = {
    null
  }

  // ---- unsupported / result set ---------------------------------------------

  /**
   * INTERNAL
   */
  def clearWarnings() {
    // nothing to do
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def afterLast() {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def cancelRowUpdates() {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateNull(columnLabel: String) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def deleteRow() {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def insertRow() {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def moveToCurrentRow() {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def moveToInsertRow() {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def refreshRow() {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateRow() {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def first(): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def isAfterLast(): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def isBeforeFirst(): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def isFirst(): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def isLast(): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def last(): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def previous(): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def rowDeleted(): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def rowInserted(): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def rowUpdated(): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def setFetchDirection(direction: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def setFetchSize(rows: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateNull(columnIndex: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def absolute(row: Int): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def relative(offset: Int): Boolean = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateByte(columnIndex: Int, x: Byte) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateDouble(columnIndex: Int, x: Double) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateFloat(columnIndex: Int, x: Float) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateInt(columnIndex: Int, x: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateLong(columnIndex: Int, x: Long) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateShort(columnIndex: Int, x: Short) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateBoolean(columnIndex: Int, x: Boolean) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateBytes(columnIndex: Int, x: Array[Byte]) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  def getAsciiStream(columnIndex: Int): InputStream = {
    return null;
  }

  /**
   * INTERNAL
   */
  def getBinaryStream(columnIndex: Int): InputStream = {
    return null;
  }

  /**
   * @deprecated INTERNAL
   */
  def getUnicodeStream(columnIndex: Int): InputStream = {
    return null;
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getCharacterStream(columnIndex: Int): Reader = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateCharacterStream(columnIndex: Int, x: Reader, length: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateObject(columnIndex: Int, x: Object) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateObject(columnIndex: Int, x: Object, scale: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getCursorName: String = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateString(columnIndex: Int, x: String) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateByte(columnLabel: String, x: Byte) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateDouble(columnLabel: String, x: Double) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateFloat(columnLabel: String, x: Float) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateInt(columnLabel: String, x: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateLong(columnLabel: String, x: Long) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateShort(columnLabel: String, x: Short) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateBoolean(columnLabel: String, x: Boolean) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateBytes(columnLabel: String, x: Array[Byte]) {
    throw DbException.getUnsupportedException
  }

  /**
   * @deprecated INTERNAL
   */
  @throws(classOf[SQLException])
  def getBigDecimal(columnIndex: Int, scale: Int): BigDecimal = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateBigDecimal(columnIndex: Int, x: BigDecimal) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getURL(columnIndex: Int): URL = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateArray(columnIndex: Int, x: java.sql.Array) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getBlob(i: Int): Blob = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateBlob(columnIndex: Int, x: Blob) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getClob(i: Int): Clob = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateClob(columnIndex: Int, x: Clob) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateDate(columnIndex: Int, x: Date) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getRef(i: Int): Ref = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateRef(columnIndex: Int, x: Ref) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateTime(columnIndex: Int, x: Time) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateTimestamp(columnIndex: Int, x: Timestamp) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getAsciiStream(columnLabel: String): InputStream = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getBinaryStream(columnLabel: String): InputStream = {
    throw DbException.getUnsupportedException
  }

  /**
   * @deprecated INTERNAL
   */
  @throws(classOf[SQLException])
  def getUnicodeStream(columnLabel: String): InputStream = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateAsciiStream(columnLabel: String, x: InputStream, length: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateBinaryStream(columnLabel: String, x: InputStream, length: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getCharacterStream(columnLabel: String): Reader = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateCharacterStream(columnLabel: String, reader: Reader, length: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateObject(columnLabel: String, x: Object) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateObject(columnLabel: String, x: Object, scale: Int) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getObject(i: Int, map: Map[String, Class[_]]): Object = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateString(columnLabel: String, x: String) {
    throw DbException.getUnsupportedException
  }

  /**
   * @deprecated INTERNAL
   */
  @throws(classOf[SQLException])
  def getBigDecimal(columnLabel: String, scale: Int): BigDecimal = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateBigDecimal(columnLabel: String, x: BigDecimal) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getURL(columnLabel: String): URL = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateArray(columnLabel: String, x: java.sql.Array) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getBlob(colName: String): Blob = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateBlob(columnLabel: String, x: Blob) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getClob(colName: String): Clob = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateClob(columnLabel: String, x: Clob) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateDate(columnLabel: String, x: Date) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getDate(columnIndex: Int, cal: Calendar): Date = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getRef(colName: String): Ref = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateRef(columnLabel: String, x: Ref) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateTime(columnLabel: String, x: Time) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getTime(columnIndex: Int, cal: Calendar): Time = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def updateTimestamp(columnLabel: String, x: Timestamp) {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getObject(colName: String, map: Map[String, Class[_]]): Object = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getDate(columnLabel: String, cal: Calendar): Date = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getTime(columnLabel: String, cal: Calendar): Time = {
    throw DbException.getUnsupportedException
  }

  /**
   * INTERNAL
   */
  @throws(classOf[SQLException])
  def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = {
    throw DbException.getUnsupportedException
  }

  // --- private -----------------------------

 
  @throws(classOf[SQLException])
  private def checkColumnIndex(columnIndex: Int) {
    if (columnIndex < 0 || columnIndex >= _columns.size()) {
      DbException.getSqlException("Invalid value at column index: " + (columnIndex + 1), ErrorCode.INVALID_VALUE_2)
    }
  }

  @throws(classOf[SQLException])
  private def get(columnIndex: Int): Any = {
    if (_currentRow == null) {
      DbException.getSqlException("No data available at column index: " + (columnIndex + 1), ErrorCode.NO_DATA_AVAILABLE)
    }
    val columnIndex1 = columnIndex - 1
    checkColumnIndex(columnIndex1)
    val o = if (columnIndex1 < _currentRow.length) _currentRow(columnIndex1) else null
    _wasNull = o == null
    o
  }
  
  @throws(classOf[SQLException])
  private def getColumn(i: Int): Column = {
    checkColumnIndex(i)
    _columns.get(i)
  }

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def getRowId(columnIndex: Int): RowId = {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def getRowId(columnLabel: String): RowId = {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateRowId(columnIndex: Int, x: RowId) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateRowId(columnLabel: String, x: RowId) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * Returns the current result set holdability.
   *
   * @return the holdability
   */
//## Java 1.4 begin ##
  def getHoldability(): Int = {
    ResultSet.HOLD_CURSORS_OVER_COMMIT
  }
//## Java 1.4 end ##

  /**
   * Returns whether this result set has been closed.
   *
   * @return true if the result set was closed
   */
  def isClosed(): Boolean = {
    _rows == null
  }

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNString(columnIndex: Int, nString: String) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNString(columnLabel: String, nString: String) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNClob(columnIndex: Int, nClob: NClob) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNClob(columnLabel: String, nClob: NClob) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def getNClob(columnIndex: Int): NClob = {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def getNClob(columnLabel: String): NClob = {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def getSQLXML(columnIndex: Int): SQLXML = {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def getSQLXML(columnLabel: String): SQLXML = {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateSQLXML(columnIndex: Int, xmlObject: SQLXML) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateSQLXML(columnLabel: String, xmlObject: SQLXML) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def getNString(columnIndex: Int): String = {
    return getString(columnIndex);
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def getNString(columnLabel: String): String = {
    return getString(columnLabel);
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def getNCharacterStream(columnIndex: Int): Reader = {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def getNCharacterStream(columnLabel: String): Reader = {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def unwrap[T](iface: Class[T]): T = {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def isWrapperFor(iface: Class[_]): Boolean = {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateAsciiStream(columnIndex: Int, x: InputStream)
  {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateAsciiStream(columnLabel: String, x: InputStream) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateAsciiStream(columnLabel: String, x: InputStream, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateBinaryStream(columnIndex: Int, x: InputStream) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateBinaryStream(columnLabel: String, x: InputStream) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateBinaryStream(columnLabel: String, x: InputStream, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateBlob(columnIndex: Int, x: InputStream) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateBlob(columnLabel: String, x: InputStream) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateBlob(columnIndex: Int, x: InputStream, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateBlob(columnLabel: String, x: InputStream, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateCharacterStream(columnIndex: Int, x: Reader) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateCharacterStream(columnLabel: String, x: Reader) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateCharacterStream(columnIndex: Int, x: Reader, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateCharacterStream(columnLabel: String, x: Reader, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateClob(columnIndex: Int, x: Reader) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateClob(columnLabel: String, x: Reader) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateClob(columnIndex: Int, x: Reader, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateClob(columnLabel: String, x: Reader, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNCharacterStream(columnIndex: Int, x: Reader) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNCharacterStream(columnLabel: String, x: Reader) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNCharacterStream(columnLabel: String, x: Reader, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNClob(columnIndex: Int, x: Reader) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNClob(columnLabel: String, x: Reader) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNClob(columnIndex: Int, x: Reader, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * INTERNAL
   */
//## Java 1.6 begin ##
  @throws(classOf[SQLException])
  def updateNClob(columnLabel: String, x: Reader, length: Long) {
    throw DbException.getUnsupportedException
  }
//## Java 1.6 end ##

  /**
   * Set the auto-close behavior. If enabled (the default), the result set is
   * closed after reading the last row.
   *
   * @param autoClose the new value
   */
  def setAutoClose(autoClose: Boolean) {
    this._autoClose = autoClose
  }

  /**
   * Get the current auto-close behavior.
   *
   * @return the auto-close value
   */
  def getAutoClose(): Boolean = {
    _autoClose
  }

}
