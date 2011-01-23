package ru.circumflex.orm

import ORM._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.sql.ResultSet
import java.util.Date
import scala.xml.NodeSeq
import scala.xml.XML

/**
 * Each field of persistent class correspond to a field of record in a relation.
 * R: type Of Record
 * T: type of field value
 */
class Field[R, T](val relation: Relation[R],
                  val name: String,
                  val sqlType: String
) extends SQLable {

  val uuid = relation.uuid + "." + name

  def read(rs: ResultSet, alias: String): Option[T] = {
    val o = rs.getObject(alias)
    if (rs.wasNull) None
    else Some(o.asInstanceOf[T])
  }

  def REFERENCES[F](toRelation: Relation[F]): Association[R, F] =
    new Association[R, F](relation, name, toRelation)

  def toSql = dialect.columnDefinition(this)

  // Should the `UNIQUE` constraint be generated for this field?
  protected var _unique: Boolean = false
  // Should the `NOT NULL` constraint be applied to this value holder?
  protected var _notNull: Boolean = true
  // An optional default expression for DDL.
  protected var _defaultExpression: Option[String] = None

  protected lazy val recField: Option[ClassVariable[R, _]] = relation.recFieldOf(this)

  def unique_?() = _unique
  def UNIQUE(): this.type = {
    _unique = true
    this
  }

  def notNull_?(): Boolean = _notNull
  def NOT_NULL(): this.type = {
    _notNull = true
    this
  }

  def defaultExpression: Option[String] = _defaultExpression
  def DEFAULT(expr: String): this.type = {
    _defaultExpression = Some(expr)
    this
  }

  def empty_?(record: R): Boolean = getValue(record) == null
  def null_?(record: R): Boolean = empty_?(record)

  def getValue(from: R): T = {
    try {
      recField match {
        case Some(x) => x.getter.invoke(from).asInstanceOf[T]
        case None => null.asInstanceOf[T]
      }
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

  def setValue(to: R, value: T): Option[() => Unit] = {
    try {
      recField match {
        case Some(x) =>
          //val o1 = Utils.convert(v, classField.getType)
          x.setter.invoke(to, value.asInstanceOf[AnyRef]) // @todo, T is any
          None
        case None => None
      }
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

  override def toString = name
}

trait AutoIncrementable[R, T] extends Field[R, T] {
  protected var _autoIncrement: Boolean = false
  def autoIncrement_? : Boolean = _autoIncrement
  def AUTO_INCREMENT: this.type = {
    _autoIncrement = true
    this
  }
}

class AutoPrimaryKeyField[R](relation: Relation[R]
) extends LongField(relation, "id") {
  _autoIncrement = true
  override def defaultExpression = Some(dialect.defaultExpression(this))
}

abstract class XmlSerializableField[R, T](relation: Relation[R], name: String, sqlType: String
) extends Field[R, T](relation, name, sqlType) with XmlSerializable[T] {
  def toXml(value: T) = value.toString
}

class TinyintField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Byte](relation, name, dialect.tinyintType) {
  def fromXml(string: String) = string.toByte
}

class IntField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Int](relation, name, dialect.integerType) with AutoIncrementable[R, Int] {
  def fromXml(string: String) = string.toInt
}

class LongField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Long](relation, name, dialect.longType) with AutoIncrementable[R, Long] {
  def fromXml(string: String) = string.toLong
}

class FloatField[R](relation: Relation[R], name: String, precision: Int = -1, scale: Int = 0
) extends XmlSerializableField[R, Float](relation, name, dialect.floatType(precision, scale)) {
  def fromXml(string: String) = string.toFloat
}

class DoubleField[R](relation: Relation[R], name: String, precision: Int = -1, scale: Int = 0
) extends XmlSerializableField[R, Double](relation, name, dialect.doubleType(precision, scale)) {
  def fromXml(string: String) = string.toDouble
}

class NumericField[R](relation: Relation[R], name: String, precision: Int = -1, scale: Int = 0
) extends XmlSerializableField[R, Double](relation, name, dialect.numericType(precision, scale)) {
  def fromXml(string: String) = string.toDouble
}

class TextField[R](relation: Relation[R], name: String, sqlType: String
) extends XmlSerializableField[R, String](relation, name, sqlType) {
  def this(relation: Relation[R], name: String, length: Int = -1) = this(relation, name, dialect.varcharType(length))
  def fromXml(string: String) = string
}

class VarbinaryField[R](relation: Relation[R], name: String, sqlType: String
) extends XmlSerializableField[R, Array[Byte]](relation, name, sqlType) {
  def this(relation: Relation[R], name: String, length: Int = -1) = this(relation, name, dialect.varbinaryType(length))
  def fromXml(string: String) = string.getBytes
}

class BooleanField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Boolean](relation, name, dialect.booleanType) {
  def fromXml(string: String) = string.toBoolean
}

class TimestampField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Date](relation, name, dialect.timestampType) {
  def fromXml(string: String) = new Date(java.sql.Timestamp.valueOf(string).getTime)
  override def toXml(value: Date) = new java.sql.Timestamp(value.getTime).toString
}

class DateField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Date](relation, name, dialect.dateType) {
  def fromXml(string: String) = new Date(java.sql.Date.valueOf(string).getTime)
  override def toXml(value: Date) = new java.sql.Date(value.getTime).toString
}

class TimeField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Date](relation, name, dialect.timeType) {
  def fromXml(string: String) = new Date(java.sql.Time.valueOf(string).getTime)
  override def toXml(value: Date) = new java.sql.Time(value.getTime).toString
}

class XmlField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, NodeSeq](relation, name, dialect.xmlType) {
  def fromXml(str: String): NodeSeq = try XML.loadString(str) catch {case _ => null}
  override def read(rs: ResultSet, alias: String) = Option(fromXml(rs.getString(alias)))
}

class SerializedField[R, T](relation: Relation[R], name: String, tpe: Class[T], length: Int = -1
) extends Field[R, Array[Byte]](relation, name, dialect.varbinaryType(length)) {

  override def getValue(from: R): Array[Byte] = {
    val v = try {
      recField match {
        case Some(x) => x.getter.invoke(from).asInstanceOf[T]
        case None => null.asInstanceOf[T]
      }
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
    encodeValue(v)
  }

  override def setValue(to: R, value: Array[Byte]) = {
    try {
      recField match {
        case Some(x) =>
          val v = decodeValue(value)
          x.setter.invoke(to, v.asInstanceOf[AnyRef])
          None
        case None => None
      }
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }


  private def encodeValue(v: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dos = new ObjectOutputStream(baos)
    try {
      dos.writeObject(v)
    } catch {
      case ioe: IOException =>
    }

    baos.toByteArray
  }

  private def decodeValue(bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new ObjectInputStream(bais)
    try {
      dis.readObject.asInstanceOf[T]
    } catch {
      case ioe: IOException => null.asInstanceOf[T]
    }
  }

}



