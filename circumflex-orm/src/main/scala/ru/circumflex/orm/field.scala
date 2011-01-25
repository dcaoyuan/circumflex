package ru.circumflex.orm

import ORM._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.nio.ByteBuffer
import java.sql.ResultSet
import java.util.Date
import org.apache.avro.{Schema => AvroSchema}
import scala.xml.NodeSeq
import scala.xml.XML

/**
 * Each field of persistent class correspond to a field of record in a relation.
 * R: type Of Record
 * T: type of field value
 */
class Field[R, T](val relation: Relation[R],
                  val name: String,
                  val sqlType: String,
                  val avroType: AvroSchema.Type
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
    _getValue(from).asInstanceOf[T]
  }
  
  def setValue(to: R, value: Any): Option[() => Unit] = {
    _setValue(to, value)
    None
  }


  /**
   * @Note usually override getValue/setValue instead of this method
   * Plain _getValue/_setValue is just used to get/set field value via reflect,
   * It's value is dealing with the true type of field of the record Object, which
   * may not be the same type of table field.
   */
  protected[orm] def _getValue(from: R): Any = {
    recField match {
      case Some(x) =>
        try {
          x.getter.invoke(from)
        } catch {
          case e: Exception => throw new RuntimeException(e)
        }
      case None => null
    }
  }

  protected[orm] def _setValue(to: R, value: Any) {
    recField match {
      case Some(x) =>
        try {
          x.setter.invoke(to, value.asInstanceOf[AnyRef])
        } catch {
          case e: Exception => throw new RuntimeException(e)
        }
      case None =>
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

abstract class XmlSerializableField[R, T](relation: Relation[R], name: String, sqlType: String, avroType: AvroSchema.Type
) extends Field[R, T](relation, name, sqlType, avroType) with XmlSerializable[T] {
  def toXml(value: T) = value.toString
}

class TinyintField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Byte](relation, name, dialect.tinyintType, AvroSchema.Type.INT) {
  def fromXml(string: String) = string.toByte
}

class IntField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Int](relation, name, dialect.integerType, AvroSchema.Type.INT) with AutoIncrementable[R, Int] {
  def fromXml(string: String) = string.toInt
}

class LongField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Long](relation, name, dialect.longType, AvroSchema.Type.LONG) with AutoIncrementable[R, Long] {
  def fromXml(string: String) = string.toLong
}

class FloatField[R](relation: Relation[R], name: String, precision: Int = -1, scale: Int = 0
) extends XmlSerializableField[R, Float](relation, name, dialect.floatType(precision, scale), AvroSchema.Type.FLOAT) {
  def fromXml(string: String) = string.toFloat
}

class DoubleField[R](relation: Relation[R], name: String, precision: Int = -1, scale: Int = 0
) extends XmlSerializableField[R, Double](relation, name, dialect.doubleType(precision, scale), AvroSchema.Type.DOUBLE) {
  def fromXml(string: String) = string.toDouble
}

class NumericField[R](relation: Relation[R], name: String, precision: Int = -1, scale: Int = 0
) extends XmlSerializableField[R, Double](relation, name, dialect.numericType(precision, scale), AvroSchema.Type.DOUBLE) {
  def fromXml(string: String) = string.toDouble
}

class TextField[R](relation: Relation[R], name: String, sqlType: String
) extends XmlSerializableField[R, String](relation, name, sqlType, AvroSchema.Type.STRING) {
  def this(relation: Relation[R], name: String, length: Int = -1) = this(relation, name, dialect.varcharType(length))
  def fromXml(string: String) = string
}

class VarbinaryField[R](relation: Relation[R], name: String, sqlType: String
) extends XmlSerializableField[R, Array[Byte]](relation, name, sqlType, AvroSchema.Type.BYTES) {
  def this(relation: Relation[R], name: String, length: Int = -1) = this(relation, name, dialect.varbinaryType(length))
  def fromXml(string: String) = string.getBytes
}

class BooleanField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Boolean](relation, name, dialect.booleanType, AvroSchema.Type.BOOLEAN) {
  def fromXml(string: String) = string.toBoolean
}

class TimestampField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Date](relation, name, dialect.timestampType, AvroSchema.Type.LONG) {
  def fromXml(string: String) = new Date(java.sql.Timestamp.valueOf(string).getTime)
  override def toXml(value: Date) = new java.sql.Timestamp(value.getTime).toString
}

class DateField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Date](relation, name, dialect.dateType, AvroSchema.Type.LONG) {
  def fromXml(string: String) = new Date(java.sql.Date.valueOf(string).getTime)
  override def toXml(value: Date) = new java.sql.Date(value.getTime).toString
}

class TimeField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, Date](relation, name, dialect.timeType, AvroSchema.Type.LONG) {
  def fromXml(string: String) = new Date(java.sql.Time.valueOf(string).getTime)
  override def toXml(value: Date) = new java.sql.Time(value.getTime).toString
}

class XmlField[R](relation: Relation[R], name: String
) extends XmlSerializableField[R, NodeSeq](relation, name, dialect.xmlType, AvroSchema.Type.STRING) {
  def fromXml(str: String): NodeSeq = try XML.loadString(str) catch {case _ => null}
  override def read(rs: ResultSet, alias: String) = Option(fromXml(rs.getString(alias)))
}

/**
 * type O is the original type of Object
 *
 * @Note:
 *   when read from rdb,  this field will be setValue by Array[Byte]
 *   when read from avro, this field will be setValue by ByteBuffer
 */
class SerializedField[R, O](relation: Relation[R], name: String, tpe: Class[O], length: Int = -1
) extends Field[R, Array[Byte]](relation, name, dialect.varbinaryType(length), AvroSchema.Type.BYTES) {

  override def getValue(from: R): Array[Byte] = {
    val trueValue = _getValue(from).asInstanceOf[O]
    encodeValue(trueValue)
  }

  override def setValue(to: R, value: Any) = {
    val bytes = value match {
      case x: ByteBuffer => x.array
      case x: Array[Byte] => x
    }
    val trueValue = decodeValue(bytes)
    super.setValue(to, trueValue)
  }

  private def encodeValue(v: O): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dos = new ObjectOutputStream(baos)
    try {
      dos.writeObject(v)
    } catch {
      case ioe: IOException =>
    }

    baos.toByteArray
  }

  private def decodeValue(bytes: Array[Byte]): O = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new ObjectInputStream(bais)
    try {
      dis.readObject.asInstanceOf[O]
    } catch {
      case ioe: IOException => null.asInstanceOf[O]
    }
  }

}



