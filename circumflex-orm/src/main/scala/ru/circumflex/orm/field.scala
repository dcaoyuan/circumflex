package ru.circumflex.orm

import ORM._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.Date

/**
 * Each field of persistent class correspond to a field of record in a relation.
 */
class Field[T](val relation: Relation[_],
               val name: String,
               val uuid: String,
               val sqlType: String
) extends SQLable {
  /* extends ValueHolder[T](name, uuid) */

  // Should the `UNIQUE` constraint be generated for this field?
  protected var _unique: Boolean = false
  // Should the `NOT NULL` constraint be applied to this value holder?
  protected var _notNull: Boolean = true
  // An optional default expression for DDL.
  protected[orm] var _defaultExpr: Option[String] = None

  protected lazy val recField: Option[ClassVariable[_]] = relation.recFieldOf(this)

  relation.addField(this)

  def unique: this.type = {
    _unique = true
    this
  }
  def UNIQUE: this.type = unique
  def unique_?() = _unique

  def nullable_?(): Boolean = !_notNull
  def notNull: this.type = {
    _notNull = true
    this
  }
  def NOT_NULL: this.type = notNull
  def nullable: this.type = {
    _notNull = false
    this
  }
  def NULLABLE: this.type = nullable

  def default = _defaultExpr
  def default(expr: String): this.type = {
    _defaultExpr = Some(dialect.defaultExpression(expr))
    this
  }
  def DEFAULT(expr: String): this.type = default(expr)

  def toSql = dialect.columnDefinition(this)

  def empty_?(record: AnyRef): Boolean = getValue(record) == null
  def null_?(record: AnyRef): Boolean = empty_?(record)
  def NULL_?(record: AnyRef): Boolean = empty_?(record)

  def getValue(from: AnyRef): T = {
    try {
      recField match {
        case Some(x) => x.getter.invoke(from).asInstanceOf[T]
        case None => null.asInstanceOf[T]
      }
    } catch {case e: Exception => throw new RuntimeException(e)}
  }

  def setValue(to: AnyRef, value: T): Option[() => Unit] = {
    try {
      recField match {
        case Some(x) =>
          //val o1 = Utils.convert(v, classField.getType)
          x.setter.invoke(to, value.asInstanceOf[AnyRef]) // @todo, T is any
          None
        case None => None
      }
    } catch {case e: Exception => throw new RuntimeException(e)}
  }
}

class PrimaryKeyField(relation: Relation[_]
) extends Field[Long](relation, "id", relation.uuid + "." + "id", dialect.longType) {
  override def default = Some(dialect.primaryKeyExpression(relation))
}

abstract class XmlSerializableField[T](relation: Relation[_],
                                       name: String, uuid: String, sqlType: String
) extends Field[T](relation, name, uuid, sqlType) with XmlSerializable[T] {
  def to(value: T) = value.toString
}

class IntField(relation: Relation[_],
               name: String, uuid: String
) extends XmlSerializableField[Int](relation, name, uuid, dialect.integerType) {
  def from(string: String) = string.toInt
}

class LongField(relation: Relation[_],
                name: String, uuid: String)
extends XmlSerializableField[Long](relation, name, uuid, dialect.longType) {
  def from(string: String) = string.toLong
}

class FloatField(relation: Relation[_],
                 name: String, uuid: String, precision: Int = -1, scale: Int = 0
) extends XmlSerializableField[Float](
  relation,
  name,
  uuid,
  dialect.floatType + (if (precision == -1) "" else "(" + precision + "," + scale + ")")) {
  def from(string: String) = string.toFloat
}

class DoubleField(relation: Relation[_],
                  name: String, uuid: String, precision: Int = -1, scale: Int = 0
) extends XmlSerializableField[Double](
  relation,
  name,
  uuid,
  dialect.doubleType + (if (precision == -1) "" else "(" + precision + "," + scale + ")")) {
  def from(string: String) = string.toDouble
}

class NumericField(relation: Relation[_],
                   name: String, uuid: String, precision: Int = -1, scale: Int = 0
) extends XmlSerializableField[Double](
  relation,
  name,
  uuid,
  dialect.numericType + (if (precision == -1) "" else "(" + precision + "," + scale + ")")) {
  def from(string: String) = string.toDouble
}

class TextField(relation: Relation[_],
                name: String, uuid: String, sqlType: String
) extends XmlSerializableField[String](relation, name, uuid, sqlType) {
  def this(relation: Relation[_], name: String, uuid: String, length: Int = -1) =
    this(relation, name, uuid, dialect.varcharType + (if (length == -1) "" else "(" + length + ")"))
  def from(string: String) = string
}

class VarbinaryField(relation: Relation[_],
                     name: String, uuid: String, sqlType: String
) extends XmlSerializableField[Array[Byte]](relation, name, uuid, sqlType) {
  def this(relation: Relation[_], name: String, uuid: String, length: Int = -1) =
    this(relation, name, uuid, dialect.varbinaryType + (if (length == -1) "" else "(" + length + ")"))
  def from(string: String) = string.getBytes
}

class SerializedField[T](relation: Relation[_],
                         name: String, uuid: String, tpe: Class[T], length: Int = -1
) extends Field[Array[Byte]](relation, name, uuid, dialect.varbinaryType + (if (length == -1) "" else "(" + length + ")")) {

  override def getValue(from: AnyRef): Array[Byte] = {
    val v = try {
      recField match {
        case Some(x) => x.getter.invoke(from).asInstanceOf[T]
        case None => null.asInstanceOf[T]
      }
    } catch {case e: Exception => throw new RuntimeException(e)}
    encodeValue(v)
  }

  override def setValue(to: AnyRef, value: Array[Byte]) = {
    try {
      recField match {
        case Some(x) =>
          val v = decodeValue(value)
          x.setter.invoke(to, v.asInstanceOf[AnyRef])
          None
        case None => None
      }
    } catch {case e: Exception => throw new RuntimeException(e)}
  }


  private def encodeValue(v: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dos = new ObjectOutputStream(baos)
    try {
      dos.writeObject(v)
    } catch {case ioe: IOException =>}

    baos.toByteArray
  }

  private def decodeValue(bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new ObjectInputStream(bais)
    try {
      dis.readObject.asInstanceOf[T]
    } catch {case ioe: IOException => null.asInstanceOf[T]}
  }

}

class BooleanField(relation: Relation[_],
                   name: String, uuid: String
) extends XmlSerializableField[Boolean](relation, name, uuid, dialect.booleanType) {
  def from(string: String) = string.toBoolean
}

class TimestampField(relation: Relation[_],
                     name: String, uuid: String
) extends XmlSerializableField[Date](relation, name, uuid, dialect.timestampType) {
  def from(string: String) = new Date(java.sql.Timestamp.valueOf(string).getTime)
  override def to(value: Date) = new java.sql.Timestamp(value.getTime).toString
}

class DateField(relation: Relation[_],
                name: String, uuid: String
) extends XmlSerializableField[Date](relation, name, uuid, dialect.dateType) {
  def from(string: String) = new Date(java.sql.Date.valueOf(string).getTime)
  override def to(value: Date) = new java.sql.Date(value.getTime).toString
}

class TimeField(relation: Relation[_],
                name: String, uuid: String
) extends XmlSerializableField[Date](relation, name, uuid, dialect.timeType) {
  def from(string: String) = new Date(java.sql.Time.valueOf(string).getTime)
  override def to(value: Date) = new java.sql.Time(value.getTime).toString
}



