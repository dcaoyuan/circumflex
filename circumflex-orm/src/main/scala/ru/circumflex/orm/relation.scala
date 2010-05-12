package ru.circumflex.orm

import ORM._
import JDBC._
import java.sql.Statement
import ru.circumflex.core.Circumflex
import ru.circumflex.core.CircumflexUtil._
import java.sql.PreparedStatement
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.WeakHashMap

// ## Relations registry

/**
 * This singleton holds mappings between `Record` classes and their
 * corresponding relations. This provides weak coupling between the two
 * to allow proper initialization of either side.
 */
object RelationRegistry {

  protected var classToRelation: Map[Class[_], Relation[_]] = Map()

  def getRelation[R <: AnyRef](r: R): Relation[R] =
    classToRelation.get(r.getClass) match {
      case Some(rel: Relation[R]) => rel
      case _ => {
          val relClass = Circumflex.loadClass[Relation[R]](r.getClass.getName + "$")
          val relation = relClass.getField("MODULE$").get(null).asInstanceOf[Relation[R]]
          classToRelation += (r.getClass -> relation)
          relation
        }
    }

}


// ## Relation

abstract class Relation[R <: AnyRef](implicit m: Manifest[R]) {

  // ### Implicits

  implicit def str2ddlHelper(name: String): DefinitionHelper[R] =
    new DefinitionHelper(this, name)

  // ### Commons

  /**
   * Attempt to find a record class by convention of companion object,
   * e.g. strip trailing `$` from `this.getClass.getName`.
   * getClass.getName.replaceAll("\\$(?=\\Z)", "")
   */
  val recordClass: Class[R] = Circumflex.loadClass[R](m.erasure.getName)

  private val recordSample: R = recordClass.newInstance
  private val recordFields = ClassUtil.getPublicVariables(recordClass)

  val recordToId = new WeakHashMap[R, Long]

  protected[orm] var _fields: Seq[Field[_]] = ListBuffer()
  protected[orm] var _associations: Seq[Association[R, _]] = ListBuffer()
  protected[orm] var _constraints: Seq[Constraint] = ListBuffer()
  protected[orm] var _preAux: Seq[SchemaObject] = ListBuffer()
  protected[orm] var _postAux: Seq[SchemaObject] = ListBuffer()

  /**
   * Unique identifier based on `recordClass` to identify this relation
   * among others.
   */
  val uuid = getClass.getName

  /**
   * Relation name defaults to record's unqualified class name, transformed
   * with `Circumflex.camelCaseToUnderscore`.
   */
  val relationName = {
    val clzName = getClass.getSimpleName
    val normalName = clzName.substring(0, clzName.length - 1) // strip ending '$'
    camelCaseToUnderscore(normalName)
  }

  /**
   * Schema is used to produce a qualified name for relation.
   */
  val schema: Schema = defaultSchema

  /**
   * Obtain a qualified name for this relation from dialect.
   */
  val qualifiedName = dialect.relationQualifiedName(this)

  /**
   * id field of this relation.
   */
  val id = new PrimaryKeyField(this)

  /**
   * Primary key field of this relation.
   */
  val primaryKey = id

  val validator = new RecordValidator(this)

  /**
   * Inspect `recordClass` to find fields and constraints definitions.
   */
  private lazy val fieldToRecField: Map[Field[_], ClassVariable[_]] = {
    var res = Map[Field[_], ClassVariable[_]]()
    ClassUtil.getValDefs(getClass) foreach {getter =>
      getter.getReturnType match {
        case tpe if classOf[Field[_]].isAssignableFrom(tpe) =>
          val field = getter.invoke(this).asInstanceOf[Field[_]]
          if (field == null) {
            ormLog.warn("Cannot get field val: " + getter)
          } else {
            recordFields find (_.name == getter.getName) match {
              case Some(recField) => res += (field -> recField)
              case None =>
            }
          }
        case tpe if classOf[Association[R, _]].isAssignableFrom(tpe) =>
          val assoc = getter.invoke(this).asInstanceOf[Association[R, _]]
          if (assoc == null) {
            ormLog.warn("Cannot get field val: " + getter)
          } else {
            recordFields find (_.name == getter.getName) match {
              case Some(recField) => res += (assoc.field -> recField)
              case None =>
            }
          }
        case tpe if classOf[InverseAssociation[_, R]].isAssignableFrom(tpe) =>
        case _ =>
      }
    }
    res
  }

  def recFieldOf(field: Field[_]): Option[ClassVariable[_]] = {
    fieldToRecField.get(field)
  }


  def fields: Seq[Field[_]] = _fields
  protected[orm] def addField(field: Field[_]) {
    _fields :+= field
    if (field.unique_?) unique(field)
  }

  def associations: Seq[Association[R, _]] = _associations
  protected[orm] def addAssociation(assoc: Association[R, _]) {
    _associations :+= assoc
    _constraints  :+= associationFK(assoc)
  }

  def constraints: Seq[Constraint] = _constraints
  def preAux: Seq[SchemaObject] = _preAux
  def postAux: Seq[SchemaObject] = _postAux

  def r: R = recordSample
  def > = r

  /**
   * Are DML statements allowed against this relation?
   */
  def readOnly_? : Boolean = false

  def idOf(record: R): Option[Long] = recordToId.get(record)
  def recordOf(id: Long): Option[R] = recordToId find {x => x._2 == id} map (_._1)

  /**
   * Yield `true` if `primaryKey` field is empty (contains `None`).
   */
  def transient_?(record: R): Boolean = recordToId.get(record).isEmpty

  /**
   * Create new `RelationNode` with specified `alias`.
   */
  def as(alias: String) = (new RelationNode[R] {
      /**
       * The following code will cause the node.relation returning the actul type
       * even when this Relation is extended by a object
       */
      val relation: Relation.this.type = Relation.this
    }).as(alias)

  /**
   * Try to find an association to specified `relation`.
   */
  def findAssociation[F <: AnyRef](relation: Relation[F]): Option[Association[R, F]] =
    associations.find(_.foreignRelation == relation).asInstanceOf[Option[Association[R, F]]]

  private def associationFK(assoc: Association[_, _]): ForeignKey = {
    val name = relationName + "_" + assoc.name + "_fkey"
    new ForeignKey(this,
                   name,
                   assoc.foreignRelation,
                   List(assoc.field),
                   List(assoc.foreignRelation.primaryKey),
                   assoc.onDelete,
                   assoc.onUpdate)
  }

  /**
   * Allow dialects to override the initialization logic.
   */
  dialect.initializeRelation(this)


  // ### Simple queries

  /**
   * Retrieve the record by specified `id` from transaction-scoped cache,
   * or fetch it from database.
   */
  def get(id: Long): Option[R] = tx.getCachedRecord(this, id) match {
    case Some(record: R) => Some(record)
    case None => as("root").criteria.add("root.id" EQ id).unique
  }

  /**
   * Fetch all records.
   */
  def all(limit: Int = -1, offset: Int = 0): Seq[R] = as("root").criteria
  .limit(limit)
  .offset(offset)
  .list
  
  // ### Definitions

  /**
   * A helper for creating named constraints.
   */
  protected[orm] def constraint(name: String): ConstraintHelper =
    new ConstraintHelper(this, name)
  protected[orm] def CONSTRAINT(name: String): ConstraintHelper = constraint(name)

  /**
   * Add a unique constraint to this relation's definition.
   */
  protected[orm] def unique(fields: Field[_]*): UniqueKey =
    constraint(relationName + "_" + fields.map(_.name).mkString("_") + "_key").unique(fields: _*)
  protected[orm] def UNIQUE(fields: Field[_]*) = unique(fields: _*)

  /**
   * Add a foreign key constraint to this relation's definition.
   */
  def foreignKey(localFields: Field[_]*): ForeignKeyHelper =
    new ForeignKeyHelper(this, relationName + "_" +
                         localFields.map(_.name).mkString("_") + "_fkey", localFields)
  def FOREIGN_KEY(localFields: Field[_]*): ForeignKeyHelper = foreignKey(localFields: _*)

  /**
   * Inverse associations.
   */
  def inverse[C <: AnyRef](association: Association[C, R]): InverseAssociation[R, C] =
    new InverseAssociation(association)

  /**
   * Add an index to this relation's definition.
   */
  def index(indexName: String, expressions: String*): Index = {
    val idx = new Index(this, indexName, expressions: _*)
    addPostAux(idx)
    return idx
  }
  def INDEX(indexName: String, expressions: String*): Index = index(indexName, expressions: _*)

  /**
   * Add specified `objects` to this relation's `preAux` queue.
   */
  def addPreAux(objects: SchemaObject*): this.type = {
    objects.foreach(o => if (!_preAux.contains(o)) _preAux :+= o)
    return this
  }

  /**
   * Add specified `objects` to this relaion's `postAux` queue.
   */
  def addPostAux(objects: SchemaObject*): this.type = {
    objects.foreach(o => if (!_postAux.contains(o)) _postAux :+= o)
    return this
  }

  // ### Persistence

  /**
   * A helper to set parameters to `PreparedStatement`.
   */
  protected[orm] def setParams(record: R, st: PreparedStatement, fields: Seq[Field[_]]) =
    (0 until fields.size).foreach(ix => typeConverter.write(st, fields(ix).getValue(record), ix + 1))

  /**
   * Uses last generated identity to refetch specified `record`.
   *
   * This method must be called immediately after `insert_!`.
   */
  def refetchLast(record: R) {
    val root = as("root")
    SELECT (root.*) FROM root WHERE (dialect.lastIdExpression(root)) unique match {
      case Some(r: R) => copyFields(r, record)
      case _ => throw new ORMException("Could not locate the last inserted row.")
    }
  }

  def refetchLast(record: R, latestId: Long) {
    val root = as("root")
    val latestIdEq = if (latestId != -1)
      root.alias + "." + root.relation.primaryKey.name + " = " + latestId
    else dialect.lastIdExpression(root)
    SELECT (root.*) FROM root WHERE (latestIdEq) unique match {
      case Some(r: R) => copyFields(r, record)
      case _ => throw new ORMException("Could not locate the last inserted row.")
    }
  }

  protected[orm] def copyFields(from: R, to: R): Unit =
    recordFields foreach {_.copyField(from, to)}

  // ### Validate, Insert, Update, Save and Delete

  /**
   * Performs record validation.
   */
  def validate(record: R): Option[Seq[ValidationError]] = {
    val errors = validator.validate(record)
    if (errors.size == 0) None
    else Some(errors)
  }

  /**
   * Skips the validation and performs `INSERT` statement for this record.
   * If no `fields` specified, performs full insert (except empty fields
   * with default values), otherwise only specified `fields` participate
   * in the statement.
   */
  def insert_!(record: R, fields: Field[_]*): Int = if (readOnly_?)
    throw new ORMException("The relation " + qualifiedName + " is read-only.")
  else transactionManager.dml{conn =>
    val fs: Seq[Field[_]] = if (fields.isEmpty) this.fields.filter(f => !f.empty_?(record)) else fields
    val sql = dialect.insertRecord(this, fs)
    sqlLog.debug(sql)
    auto(conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){st =>
      setParams(record, st, fs)
      val rows = st.executeUpdate
      val keys = st.getGeneratedKeys
      if (rows > 0 && keys.next) {
        val latestId = keys.getLong(1)
        // refresh latestId for this record
        recordToId += (record -> latestId)
      }
      rows
    }
  }
  def INSERT_!(record: R, fields: Field[_]*): Int = insert_!(record, fields: _*)

  /**
   * Validates record and executes `insert_!` on success.
   */
  def insert(record: R, fields: Field[_]*): Int = validate(record) match {
    case None => insert_!(record, fields: _*)
    case Some(errors) => throw new ValidationException(errors: _*)
  }
  def INSERT(record: R, fields: Field[R]*) = insert(record, fields: _*)

  def insertBatch_!(records: Array[R]): Int = {
    if (readOnly_?)
      throw new ORMException("The relation " + qualifiedName + " is read-only.")
    else {
      if (records.length == 0) return 0

      transactionManager.dml{conn =>
        val fs = this.fields.filter(f => f != id)
        val sql = dialect.insertRecord(this, fs)
        sqlLog.debug(sql)
        val st = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        for (record <- records) {
          setParams(record, st, fs)
          st.addBatch
        }
        val rows = st.executeBatch
        val keys = st.getGeneratedKeys
        var count = 0
        // getGeneratedKeys may return only one id (the latest)
        var latestId = if (keys.next) keys.getLong(1) else -1
        var i = records.length - 1
        while (i > 0) {
          val row = rows(i)
          if (row > 0) {
            count += 1
            if (latestId > 0) {
              // refresh latestId for this record
              recordToId += (records(i) -> latestId)
              latestId -= 1
            }
          }
          i -= 1
        }
        
        count
      }
    }
  }

  def insertBatch(records: Array[R]): Int = {
    records foreach validate
    insertBatch_!(records)
  }

  /**
   * Skips the validation and performs `UPDATE` statement for this record.
   * If no `fields` specified, performs full update, otherwise only specified
   * `fields` participate in the statement.
   */
  def update_!(record: R, fields: Field[_]*): Int = if (readOnly_?)
    throw new ORMException("The relation " + qualifiedName + " is read-only.")
  else transactionManager.dml(conn => {
      val fs: Seq[Field[_]] = if (fields.size == 0) _fields.filter(f => f != id) else fields
      val sql = dialect.updateRecord(this, fs)
      sqlLog.debug(sql)
      auto(conn.prepareStatement(sql)){st =>
        setParams(record, st, fs)
        typeConverter.write(st, idOf(record), fs.size + 1)
        st.executeUpdate
      }
    })
  def UPDATE_!(record: R, fields: Field[R]*): Int = update_!(record, fields: _*)

  /**
   * Validates record and executes `update_!` on success.
   */
  def update(record: R, fields: Field[_]*): Int = validate(record) match {
    case None => update_!(record, fields: _*)
    case Some(errors) => throw new ValidationException(errors: _*)
  }
  def UPDATE(record: R, fields: Field[_]*) = update(record, fields: _*)

  /**
   * Executes the `DELETE` statement for this record using primary key
   * as delete criteria.
   */
  def delete_!(record: R): Int = if (readOnly_?)
    throw new ORMException("The relation " + qualifiedName + " is read-only.")
  else transactionManager.dml(conn => {
      val sql = dialect.deleteRecord(this)
      sqlLog.debug(sql)
      auto(conn.prepareStatement(sql)){st =>
        typeConverter.write(st, idOf(record), 1)
        st.executeUpdate
      }
    })
  def DELETE_!(record: R): Int = delete_!(record)

  /**
   * If record's `id` field is not `NULL` perform `update`, otherwise perform `insert`
   * and then refetch record using last generated identity.
   */
  def save_!(record: R): Int = if (transient_?(record)) {
    val rows = insert_!(record)
    // why should refetch all this record when save? I have set record id at insert_!
    //refetchLast(record, latestId)
    rows
  } else update_!(record)

  /**
   * Validates record and executes `save_!` on success.
   */
  def save(record: R): Int = validate(record) match {
    case None => save_!(record)
    case Some(errors) => throw new ValidationException(errors: _*)
  }

  /**
   * Invalidates transaction-scoped cache for this record and refetches it from database.
   */
  def refresh(record: R): this.type = if (transient_?(record))
    throw new ORMException("Could not refresh transient record.")
  else {
    tx.evictRecordCache(this, record)
    val root = as("root")
    val id = idOf(record).get
    SELECT (root.*) FROM root WHERE (root.id EQ id) unique match {
      case Some(r: R) => copyFields(r, record)
      case _ =>
        throw new ORMException("Could not locate record with id = " + id + " in database.")
    }
    return this
  }

  // ### Equality and others

  override def equals(that: Any) = that match {
    case r: Relation[R] => r.relationName.equalsIgnoreCase(this.relationName)
    case _ => false
  }

  override def hashCode = this.relationName.toLowerCase.hashCode

  override def toString = qualifiedName

}

// ## Table

abstract class Table[R <: AnyRef: Manifest] extends Relation[R] with SchemaObject {
  val objectName = "TABLE " + qualifiedName
  lazy val sqlDrop = dialect.dropTable(this)
  lazy val sqlCreate = dialect.createTable(this)
}

// ## View

abstract class View[R <: AnyRef: Manifest] extends Relation[R] with SchemaObject {

  // ### Miscellaneous

  val objectName = "VIEW " + qualifiedName
  lazy val sqlDrop = dialect.dropView(this)
  lazy val sqlCreate = dialect.createView(this)

  /**
   * Views are not updatable by default.
   */
  override def readOnly_?() = true

  /**
   * A `query` that makes up this view definition.
   */
  def query: Select[_]

}


// ## Helper for fields DSL

/**
 * A tiny builder that helps to instantiate `Field`s and `Association`s
 * in a neat DSL-like way.
 */
class DefinitionHelper[R <: AnyRef](relation: Relation[R], name: String) {

  def uuid = relation.getClass.getName + "." + name

  def integer = new IntField(relation, name, uuid)
  def bigint = new LongField(relation, name, uuid)
  def float(precision: Int = -1, scale: Int = 0) = new FloatField(relation, name, uuid, precision, scale)
  def double(precision: Int = -1, scale: Int = 0) = new DoubleField(relation, name, uuid, precision, scale)
  def numeric(precision: Int = -1, scale: Int = 0) = new NumericField(relation, name, uuid, precision, scale)
  def text = new TextField(relation, name, uuid, dialect.textType)
  def varchar(length: Int = -1) = new TextField(relation, name, uuid, length)
  def varbinary(length: Int = -1) = new VarbinaryField(relation, name, uuid, length)
  def boolean = new BooleanField(relation, name, uuid)
  def date = new DateField(relation, name, uuid)
  def time = new TimeField(relation, name, uuid)
  def timestamp = new TimestampField(relation, name, uuid)

  def INTEGER = integer
  def BIGINT = bigint
  def FLOAT(precision: Int = -1, scale: Int = 1) = float(precision, scale)
  def DOUBLE(precision: Int = -1, scale: Int = 1) = double(precision, scale)
  def NUMERIC(precision: Int = -1, scale: Int = 1) = numeric(precision, scale)
  def TEXT = text
  def VARCHAR(length: Int = -1) = varchar(length)
  def VARBINARY(length: Int = -1) = varbinary(length)
  def BOOLEAN = boolean
  def DATE = date
  def TIME = time
  def TIMESTAMP = timestamp

  def references[F <: AnyRef](toRelation: Relation[F]): Association[R, F] =
    new Association[R, F](relation, name, uuid, toRelation)
  def REFERENCES[F <: AnyRef](toRelation: Relation[F]): Association[R, F] = references(toRelation)
}
