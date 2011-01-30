package ru.circumflex.orm

import net.lag.logging.Logger
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

// ## Common interfaces

/**
 * Simple interface for objects capable to render themselves into SQL statements.
 */
trait SQLable {
  def toSql: String
}

/**
 * Simple interface for expressions with JDBC-style parameters
 */
trait ParameterizedExpression extends SQLable {
  /**
   * The parameters associated with this expression. The order is important.
   */
  def parameters: Seq[Any]

  /**
   * Render this query by replacing parameter placeholders with actual values.
   */
  def toInlineSql: String = parameters.foldLeft(toSql)((sql, p) =>
    sql.replaceFirst("\\?", ORM.typeConverter.escape(p)))

  // Equality and others.

  override def equals(that: Any) = that match {
    case e: ParameterizedExpression =>
      e.toSql == this.toSql && (e.parameters.toList -- this.parameters.toList) == Nil
    case _ => false
  }

  override def hashCode = 0

  override def toString = toSql
}

/**
 * Simple interface for database objects capable to render themselves into DDL
 * CREATE and DROP statements.
 */
trait SchemaObject {
  /**
   * SQL statement to create this database object.
   */
  def sqlCreate: String

  /**
   * SQL statement to drop this database object.
   */
  def sqlDrop: String

  /**
   * SQL object name. It is used to uniquely identify this object
   * during schema creation by `DDL` to avoid duplicates and to print
   * nice messages on schema generation.
   *
   * We follow default convention to name objects:
   *
   *     <TYPE OF OBJECT> <qualified_name>
   *
   * where `TYPE OF OBJECT` is `TABLE`, `VIEW`, `SEQUENCE`, `TRIGGER`,
   * `FUNCTION`, `PROCEDURE`, `INDEX`, etc. (note the upper case), and
   * `qualified_name` is object's unique identifier.
   *
   * For equality testing, object names are taken in case-insensitive manner
   * (e.g. `MY_TABLE` and `my_table` are considered equal).
   */
  def objectName: String

  override def hashCode = objectName.toLowerCase.hashCode

  override def equals(obj: Any) = obj match {
    case so: SchemaObject => so.objectName.equalsIgnoreCase(this.objectName)
    case _ => false
  }

  override def toString = objectName
}

/**
 * *Value holder* is designed to be an extensible atomic data carrier unit
 * of record. It is subclassed by 'Field' and 'Association'.
 */
abstract class ValueHolder[T](val name: String, val uuid: String) extends WrapperModel {

  // An internally stored value.
  protected var _value: T = _

  protected var _setter: T => T = t => t

  // This way the value will be unwrapped by FTL engine.
  def item = getValue

  // Should the `NOT NULL` constraint be applied to this value holder?
  protected var _notNull: Boolean = true
  def nullable_?(): Boolean = !_notNull

  def notNull: this.type = {
    _notNull = true
    return this
  }
  def NOT_NULL: this.type = notNull

  def nullable: this.type = {
    _notNull = false
    return this
  }
  def NULLABLE: this.type = nullable

  def setter = _setter
  def setter(sf: T => T): this.type = {
    _setter = sf
    return this
  }
  def SETTER(sf: T => T): this.type = setter(sf)

  // Getters.

  def getValue(): T = _value
  def apply(): T = getValue
  def getOrElse(default: T) = get().getOrElse(default)
  def get(): Option[T] = getValue match {
    case null => None
    case value => Some(value)
  }

  def empty_?(): Boolean = getValue() == null
  def null_?(): Boolean = empty_?
  def NULL_?(): Boolean = empty_?

  // Setters.

  def setValue(newValue: T): this.type = {
    _value = if (newValue != null) _setter(newValue) else newValue
    return this
  }
  def :=(newValue: T): this.type = setValue(newValue)
  def update(newValue: T): this.type = setValue(newValue)

  def setNull(): this.type = setValue(null.asInstanceOf[T])
  def null_!() = setNull()
  def NULL_!() = null_!()

  // Equality methods.

  override def equals(that: Any) = that match {
    case vh: ValueHolder[T] => vh.uuid == this.uuid
    case _ => false
  }

  override def hashCode = this.uuid.hashCode

  /**
   * Return a `String` representation of internal value.
   */
  def toString(default: String = "") = if (getValue == null) default else getValue.toString

  /**
   * Return `uuid` as this holder's identifier.
   */
  override def toString = uuid
}

/**
 * An action for `ON UPDATE` and `ON DELETE` clauses of
 * foreign key definitions.
 */
case class ForeignKeyAction(val toSql: String) extends SQLable {
  override def toString = toSql
}

/**
 * Join types for use in `FROM` clauses of SQL queries.
 */
case class JoinType(val toSql: String) extends SQLable {
  override def toString = toSql
}

/**
 * Set operations for use in SQL queries.
 */
case class SetOperation(val toSql: String) extends SQLable {
  override def toString = toSql
}

/**
 * An expression to use in `ORDER BY` clause.
 */
class Order(val expression: String, val parameters: Seq[Any])
extends ParameterizedExpression {

  // Specificator (`ASC` or `DESC`).

  protected[orm] var _specificator = ORM.dialect.asc

  def asc: this.type = {
    this._specificator = ORM.dialect.asc
    return this
  }
  def ASC: this.type = asc

  def desc: this.type = {
    this._specificator = ORM.dialect.desc
    return this
  }
  def DESC: this.type = desc

  // Miscellaneous.

  def toSql = expression + " " + _specificator
}

// ## JDBC utilities

/**
 * Helper constructions that automatically close such JDBC objects as
 * `ResultSet`s and `PreparedStatement`s.
 */
object JDBC {
  protected[orm] val sqlLog = Logger.get("ru.circumflex.orm")

  def autoClose[A <: {def close(): Unit}, B](obj: A)(actions: A => B)(errors: Throwable => B): B =
    try {
      return actions(obj)
    } catch {
      case e =>
        sqlLog.warning(e, e.getMessage)
        return errors(e)
    } finally {
      obj.close
    }

  def auto[A <: {def close(): Unit}, B](obj: A)(actions: A => B): B =
    autoClose(obj)(actions)(throw _)
}

// ## Exceptions

/**
 * The most generic exception class.
 */
class ORMException(msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
  def this(cause: Throwable) = this(null, cause)

  /**
   * Performs a grouping operation on a collection. The result is a map
   * in which keys are obtained via predicate function `predicateFunc` and
   * values are subcollections, every element of which satisfies the predicate.
   */
  def groupBy[K,V](it: Iterable[V], predicateFunc: V => K): collection.Map[K, Seq[V]] = {
    val result = new HashMap[K, ListBuffer[V]] {
      override def default(a: K) = new ListBuffer[V]
    }
    it foreach {v =>
      val key = predicateFunc(v)
      val buffer: ListBuffer[V] = result(key)
      buffer += v
      result += (key -> buffer)
    }
    result
  }
}

/**
 * A very simple model for operating with structural data.
 */
trait HashModel {
  def get(key: String): Option[Any]
  def apply(key: String): Any = get(key).get
  def getOrElse[A](key: String, default: =>A): A = get(key) match {
    case Some(value: A) => value;
    case _ => default
  }
}

/**
 * A very simple model for operating with wrappers.
 */
trait WrapperModel {
  def item: Any
}

