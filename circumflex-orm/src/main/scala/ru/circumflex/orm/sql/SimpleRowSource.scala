package ru.circumflex.orm.sql

/**
 * This interface is for classes that create rows on demand.
 * It is used together with SimpleResultSet to create a dynamic result set.
 */
import java.sql.SQLException

trait SimpleRowSource {

  /**
   * Get the next row. Must return null if no more rows are available.
   *
   * @return the row or null
   * @throws SQLException
   */
  @throws(classOf[SQLException])
  def readRow: Array[Object]

  /**
   * Close the row source.
   */
  def close

  /**
   * Reset the position (before the first row).
   *
   * @throws SQLException if this operation is not supported
   */
  @throws(classOf[SQLException])
  def reset
}
