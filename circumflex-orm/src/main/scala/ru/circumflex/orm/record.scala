package ru.circumflex.orm

import JDBC._

// ## Record

/**
 * *Record* is a cornerstone of relational model. In general, each instance of
 * pesistent class is stored as a single record in a relation corresponding to that
 * class. Since Circumflex ORM employs the Active Relation design approach to
 * persistence, each persistent class should subclass `Record`.
 */
abstract class Record[R <: Record[R]] { this: R =>

  // ### Commons

  /**
   * Unique identifier based on fully-qualified class name of
   * this record, which is used to uniquely identify this record
   * class among others.
   */
  val uuid = getClass.getName

  /**
   * A `Relation[R]` corresponding to this record.
   *
   * In general the relations should be the companion objects of records:
   *
   *     class Country extends Record[Country]  {
   *       val name = TEXT NOT_NULL
   * }
   *
   *     object Country extends Table[Country]
   *
   * However, if you prefer different naming conventions, you should override
   * this method.
   */
  def relation = RelationRegistry.getRelation(this)

  /**
   * We only support auto-generated `BIGINT` columns as primary keys
   * for a couple of reasons. Sorry.
   */
  //val id = new PrimaryKeyField(relation)

  /**
   * Validator for this record.
   */
  //val validation = new RecordValidator(this)

  /**
   * Yield `true` if `primaryKey` field is empty (contains `None`).
   */
  //def transient_?(): Boolean = id() == None

  /**
   * Non-DSL field creation.
   */
//  def field[T](name: String, sqlType: String) =
//    new Field[T](name, uuid + "." + name, null, null, classOf[Any], sqlType)


  // ### Miscellaneous

  /**
   * Get all fields of current record (involves some reflection).
   */
//  protected[orm] lazy val _fields: Seq[Field[_]] = relation.fields
//  .map(f => relation.methodsMap(f).invoke(this) match {
//      case f: Field[_] => f
//      case a: Association[_, _] => a.field
//      case m => throw new ORMException("Unknown member: " + m)
//    })

  /**
   * Set a specified `value` to specified `holder`.
   */
//  def setValue(vh: ValueHolder[_], value: Any): Unit = value match {
//    case Some(value) => setValue(vh, value)
//    case None => setValue(vh, null)
//    case _ => vh match {
//      case f: Field[Any] => f.setValue(value)
//      case a: Association[_, _] => value match {
//        case id: Long => setValue(a.field, id)
//        case rec: Record[_] => setValue(a.field, rec.id())
//        case _ => throw new ORMException("Could not set value " + value +
//            " to association " + a + ".")
//      }
//      case _ => throw new ORMException("Could not set value " + value +
//          " to specified value holder " + vh + ".")
//    }
//  }

  /**
   * Search for specified `field` among this record methods and set it's `value`.
   */
//  def setField[T](field: Field[T], value: T): Unit = _fields.find(f => f == field) match {
//    case Some(f: Field[T]) => f.setValue(value)
//    case _ =>
//  }
//
//  override def toString = getClass.getSimpleName + "@" + id.toString("TRANSIENT")

}
