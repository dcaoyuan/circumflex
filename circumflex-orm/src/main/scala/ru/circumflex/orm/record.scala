package ru.circumflex.orm

import ORM._
import JDBC._

// ## Record

/**
 * *Record* is a cornerstone of relational model. In general, each instance of
 * pesistent class is stored as a single record in a relation corresponding to that
 * class. Since Circumflex ORM employs the Active Relation design approach to
 * persistence, each persistent class should subclass `Record`.
 */
abstract class Record[R <: Record[R]] { this: R =>

  // ### Implicits

  implicit def str2ddlHelper(str: String): DefinitionHelper[R] =
    new DefinitionHelper(this, str)

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
  val id = new PrimaryKeyField(this)

  /**
   * Validator for this record.
   */
  val validation = new RecordValidator(this)

  /**
   * Yield `true` if `primaryKey` field is empty (contains `None`).
   */
  def transient_?(): Boolean = id() == None

  /**
   * Non-DSL field creation.
   */
  def field[T](name: String, sqlType: String) =
    new Field[T](name, uuid + "." + name, sqlType)

  /**
   * Inverse associations.
   */
  def inverse[C <: Record[C]](association: Association[C, R]): InverseAssociation[R, C] =
    new InverseAssociation(this, association)

  // ### Validate, Insert, Update, Save and Delete

  /**
   * Performs record validation.
   */
  def validate(): Option[Seq[ValidationError]] = {
    val errors = validation.validate()
    if (errors.size == 0) None
    else Some(errors)
  }

  /**
   * Skips the validation and performs `INSERT` statement for this record.
   * If no `fields` specified, performs full insert (except empty fields
   * with default values), otherwise only specified `fields` participate
   * in the statement.
   */
  def insert_!(fields: Field[_]*): Int = if (relation.readOnly_?)
    throw new ORMException("The relation " + relation.qualifiedName + " is read-only.")
  else transactionManager.dml(conn => {
    var f: Seq[Field[_]] = if (fields.size == 0) _fields.filter(f => !f.empty_?) else fields
    val sql = dialect.insertRecord(this, f)
    sqlLog.debug(sql)
    auto(conn.prepareStatement(sql))(st => {
      relation.setParams(this, st, f)
      st.executeUpdate
    })
  })
  def INSERT_!(fields: Field[_]*): Int = insert_!(fields: _*)

  /**
   * Validates record and executes `insert_!` on success.
   */
  def insert(fields: Field[_]*): Int = validate match {
    case None => insert_!(fields: _*)
    case Some(errors) => throw new ValidationException(errors: _*)
  }
  def INSERT(fields: Field[_]*) = insert(fields: _*)

  /**
   * Skips the validation and performs `UPDATE` statement for this record.
   * If no `fields` specified, performs full update, otherwise only specified
   * `fields` participate in the statement.
   */
  def update_!(fields: Field[_]*): Int = if (relation.readOnly_?)
    throw new ORMException("The relation " + relation.qualifiedName + " is read-only.")
  else transactionManager.dml(conn => {
    val f: Seq[Field[_]] = if (fields.size == 0) _fields.filter(f => f != id) else fields
    val sql = dialect.updateRecord(this, f)
    sqlLog.debug(sql)
    auto(conn.prepareStatement(sql))(st => {
      relation.setParams(this, st, f)
      typeConverter.write(st, id(), f.size + 1)
      st.executeUpdate
    })
  })
  def UPDATE_!(fields: Field[_]*): Int = update_!(fields: _*)

  /**
   * Validates record and executes `update_!` on success.
   */
  def update(fields: Field[_]*): Int = validate match {
    case None => update_!(fields: _*)
    case Some(errors) => throw new ValidationException(errors: _*)
  }
  def UPDATE(fields: Field[_]*) = update(fields: _*)

  /**
   * Executes the `DELETE` statement for this record using primary key
   * as delete criteria.
   */
  def delete_!(): Int = if (relation.readOnly_?)
    throw new ORMException("The relation " + relation.qualifiedName + " is read-only.")
  else transactionManager.dml(conn => {
    val sql = dialect.deleteRecord(this)
    sqlLog.debug(sql)
    auto(conn.prepareStatement(sql))(st => {
      typeConverter.write(st, id(), 1)
      st.executeUpdate
    })
  })
  def DELETE_!(): Int = delete_!()

  /**
   * If record's `id` field is not `NULL` perform `update`, otherwise perform `insert`
   * and then refetch record using last generated identity.
   */
  def save_!(): Int = if (transient_?) {
    val rows = insert_!()
    relation.refetchLast(this)
    return rows
  } else update_!()

  /**
   * Validates record and executes `save_!` on success.
   */
  def save(): Int = validate match {
    case None => save_!()
    case Some(errors) => throw new ValidationException(errors: _*)
  }

  /**
   * Invalidates transaction-scoped cache for this record and refetches it from database.
   */
  def refresh(): this.type = if (transient_?)
    throw new ORMException("Could not refresh transient record.")
  else {
    tx.evictRecordCache(this)
    val root = relation.as("root")
    val id = this.id.get()
    SELECT (root.*) FROM root WHERE (root.id EQ id) unique match {
      case Some(r: R) => relation.copyFields(r, this)
      case _ =>
        throw new ORMException("Could not locate record with id = " + id + " in database.")
    }
    return this
  }

  // ### Miscellaneous

  /**
   * Get all fields of current record (involves some reflection).
   */
  protected[orm] lazy val _fields: Seq[Field[_]] = relation.fields
      .map(f => relation.methodsMap(f).invoke(this) match {
    case f: Field[_] => f
    case a: Association[_, _] => a.field
    case m => throw new ORMException("Unknown member: " + m)
  })

  /**
   * Set a specified `value` to specified `holder`.
   */
  def setValue(vh: ValueHolder[_], value: Any): Unit = value match {
    case Some(value) => setValue(vh, value)
    case None => setValue(vh, null)
    case _ => vh match {
      case f: Field[Any] => f.setValue(value)
      case a: Association[_, _] => value match {
        case id: Long => setValue(a.field, id)
        case rec: Record[_] => setValue(a.field, rec.id())
        case _ => throw new ORMException("Could not set value " + value +
            " to association " + a + ".")
      }
      case _ => throw new ORMException("Could not set value " + value +
          " to specified value holder " + vh + ".")
    }
  }

  /**
   * Search for specified `field` among this record methods and set it's `value`.
   */
  def setField[T](field: Field[T], value: T): Unit = _fields.find(f => f == field) match {
    case Some(f: Field[T]) => f.setValue(value)
    case _ =>
  }

  override def toString = getClass.getSimpleName + "@" + id.toString("TRANSIENT")

}

// ## Helper for fields DSL

/**
 * A tiny builder that helps to instantiate `Field`s and `Association`s
 * in a neat DSL-like way.
 */
class DefinitionHelper[R <: Record[R]](record: R, name: String) {

  def uuid = record.uuid + "." + name

  def integer = new IntField(name, uuid)
  def bigint = new LongField(name, uuid)
  def numeric(precision: Int = -1, scale: Int = 0) = new NumericField(name, uuid, precision, scale)
  def text = new TextField(name, uuid, dialect.textType)
  def varchar(length: Int = -1) = new TextField(name, uuid, length)
  def boolean = new BooleanField(name, uuid)
  def date = new DateField(name, uuid)
  def time = new TimeField(name, uuid)
  def timestamp = new TimestampField(name, uuid)

  def INTEGER = integer
  def BIGINT = bigint
  def NUMERIC(precision: Int = -1, scale: Int = 1) = numeric(precision, scale)
  def TEXT = text
  def VARCHAR(length: Int = -1) = varchar(length)
  def BOOLEAN = boolean
  def DATE = date
  def TIME = time
  def TIMESTAMP = timestamp

  def references[F <: Record[F]](relation: Relation[F]): Association[R, F] =
    new Association[R, F](name, uuid, record, relation)
  def REFERENCES[F <: Record[F]](relation: Relation[F]): Association[R, F] =
    references(relation)
}
