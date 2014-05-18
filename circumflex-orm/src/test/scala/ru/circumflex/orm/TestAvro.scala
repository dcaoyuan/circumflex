package ru.circumflex.orm


import java.io.File
import java.sql.SQLException
import java.sql.Types
import ru.circumflex.orm.avro.Avro
import ru.circumflex.orm.sql.SimpleResultSet

object TestAvro {
  private val USER_HOME = System.getProperty("user.home")
  private val DIR = new File(System.getProperty("test.dir", USER_HOME + "/tmp"))

  def main(args: Array[String]) {
    write
    read
  }
  
  @throws(classOf[SQLException])
  def write() {
    val rs = new SimpleResultSet
    rs.addColumn("NAME", Types.VARCHAR, 255, 0)
    rs.addColumn("EMAIL", Types.VARCHAR, 255, 0)
    rs.addColumn("PHONE", Types.VARCHAR, 255, 0)
    rs.addColumn("AGE", Types.INTEGER, 255, 0)
    rs.addColumn("BIRTH", Types.BIGINT, 255, 0)
    rs.addRow("Caoyuan Deng", "caoyuan.deng@abc.abc", "+86123456789", 30, 10000L)
    rs.addRow("Guibin Zhang", "guibin.zhang@abc.abc", "+86976543210")
    Avro().write(USER_HOME + "/tmp/test.avro", rs, "test")
  }

  /**
   * Read a CSV file.
   */
  @throws(classOf[SQLException])
  def read() {
    val rs = Avro().read(USER_HOME + "/tmp/test.avro", null)
    val meta = rs.getMetaData
    while (rs.next) {
      for (i <- 0 until meta.getColumnCount) {
        println(meta.getColumnLabel(i + 1) + ": " + rs.getString(i + 1))
      }
      println
    }
    rs.close
  }

}
