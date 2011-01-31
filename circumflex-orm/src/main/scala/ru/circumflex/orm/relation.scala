package ru.circumflex.orm

import JDBC._
import java.sql.Statement
import org.aiotrade.lib.collection.WeakIdentityBiHashMap
import org.aiotrade.lib.util.config.Config
import java.lang.reflect.Method
import java.sql.PreparedStatement

// ## Relations registry

/**
 * This singleton holds mappings between `Record` classes and their
 * corresponding relations. This provides weak coupling between the two
 * to allow proper initialization of either side.
 */
object RelationRegistry {

  protected var classToRelation: Map[Class[_], Relation[_]] = Map()

  def getRelation[R](r: R): Relation[R] =
    classToRelation.get(r.asInstanceOf[AnyRef].getClass) match {
      case Some(rel: Relation[R]) => rel
      case _ => 
        val relClass = Config.loadClass[Relation[R]](r.asInstanceOf[AnyRef].getClass.getName + "$")
        val relation = relClass.getField("MODULE$").get(null).asInstanceOf[Relation[R]]
        classToRelation += (r.asInstanceOf[AnyRef].getClass -> relation)
        relation
    }
}


// ## Relation

abstract class Relation[R](implicit m: Manifest[R]) {

  protected var _initialized = false
  
  /**
   * Now this thingy is very useful and very light:
   * it translates every `ThisKindOfIdentifiers`
   * into `that_kinds_of_identifiers`.
   */
  private def camelCaseToUnderscore(arg: String) = arg.replaceAll("(?<!^)([A-Z])","_$1").toLowerCase

  // ### Implicits

  implicit def str2ddlHelper(name: String): DefinitionHelper[R] = new DefinitionHelper(this, name)

  // ### Commons

  /**
   * Attempt to find a record class by convention of companion object,
   * e.g. strip trailing `$` from `this.getClass.getName`.
   * getClass.getName.replaceAll("\\$(?=\\Z)", "")
   */
  val recordClass: Class[R] = Config.loadClass[R](m.erasure.getName)

  private val recordSample: R = recordClass.newInstance
  private val recordFields = ClassUtil.getPublicVariables(recordClass)

  private var recordToPk = WeakIdentityBiHashMap[R, Long]()

  protected var _fieldToRecField: Map[Field[R, _], ClassVariable[R, _]] = Map()
  protected var _fields: Seq[Field[R, _]] = Nil
  protected var _associations: Seq[Association[R, _]] = Nil
  protected var _constraints: Seq[Constraint[R]] = Nil
  protected var _indexes: Seq[Index[R]] = Nil
  protected var _preAuxes: Seq[SchemaObject] = Nil
  protected var _postAuxes: Seq[SchemaObject] = Nil

  /**
   * Unique identifier based on `recordClass` to identify this relation
   * among others.
   */
  val uuid = getClass.getName

  /**
   * Relation name defaults to record's unqualified class name, transformed
   * with `camelCaseToUnderscore`.
   */
  val relationName = {
    val clzName = getClass.getSimpleName
    val normalName = clzName.substring(0, clzName.length - 1) // strip ending '$'
    camelCaseToUnderscore(normalName)
  }

  /**
   * Schema is used to produce a qualified name for relation.
   */
  val schema: Schema = ORM.defaultSchema

  /**
   * Obtain a qualified name for this relation from dialect.
   */
  val qualifiedName = ORM.dialect.relationQualifiedName(this)

  val validator = new RecordValidator(this)

  /**
   * id field of this relation.
   */
  val id = new AutoPrimaryKeyField(this)

  /**
   * Primary key field of this relation.
   */
  def PRIMARY_KEY: Field[R, _] = id

  /**
   * Inspect `recordClass` to find fields and constraints definitions.
   */
  def recFieldOf(field: Field[R, _]): Option[ClassVariable[R, _]] = {
    init
    _fieldToRecField.get(field)
  }

  def fields: Seq[Field[R, _]] = {
    init
    _fields
  }

  def associations: Seq[Association[R, _]] = {
    init
    _associations
  }

  def constraints: Seq[Constraint[R]] = {
    init
    _constraints
  }

  def indexes: Seq[Index[R]] = {
    init
    _indexes
  }

  def preAuxes: Seq[SchemaObject] = _preAuxes
  def postAuxes: Seq[SchemaObject] = _postAuxes

  def r: R = recordSample
  def > = r

  /**
   * Are DML statements allowed against this relation?
   */
  def readOnly_? : Boolean = false

  def idOf(record: R): Option[Long] = {
    recordToPk.readLock.lock
    try {
      recordToPk.get(record)
    } finally {
      recordToPk.readLock.unlock
    }
  }
  def recordOf(id: Long): Option[R] = {
    recordToPk.readLock.lock
    try {
      recordToPk.getByValue(id) match {
        case None | Some(null) => None
        case some => some
      }
    } finally {
      recordToPk.readLock.unlock
    }
  }
  def updateCache(id: Long, record: R) {
    recordToPk.writeLock.lock
    try {
      recordToPk.put(record, id)
    } finally {
      recordToPk.writeLock.unlock
    }
  }
  def evictCache(record: R) {
    recordToPk.writeLock.lock
    try {
      recordToPk.remove(record)
    } finally {
      recordToPk.writeLock.unlock
    }
  }
  def evictCaches(records: Array[R]) {
    recordToPk.writeLock.lock
    try {
      var i = 0
      while (i < records.length) {
        recordToPk.remove(records(i))
        i += 1
      }
    } finally {
      recordToPk.writeLock.unlock
    }
  }
  def invalideCaches {recordToPk = WeakIdentityBiHashMap[R, Long]()}

  /**
   * Yield `true` if `primaryKey` field is empty (contains `None`).
   */
  def transient_?(record: R): Boolean = {
    recordToPk.readLock.lock
    try {
      !recordToPk.contains(record)
    } finally {
      recordToPk.readLock.unlock
    }
  }

  /**
   * Create new `RelationNode` with specified `alias`.
   */
  @deprecated("It's better not use alias, use relation name directly")
  def as(alias: String) = (new RelationNode[R] {
      /**
       * The following code will cause the node.relation returning the actul type
       * even when this Relation is extended by a object
       */
      val relation: Relation.this.type = Relation.this
    }).AS(alias)
  def AS(alias: String) = as(alias)

  /**
   * Try to find an association to specified `relation`.
   */
  def findAssociation[F](relation: Relation[F]): Option[Association[R, F]] =
    associations.find(_.foreignRelation == relation).asInstanceOf[Option[Association[R, F]]]


  private def findMembers(cl: Class[_]) {
    ClassUtil.getValDefs(cl) foreach processMember
  }

  private def processMember(getter: Method) {
    val cl = getter.getReturnType
    if (classOf[Field[R, _]].isAssignableFrom(cl)) {
      val field = getter.invoke(this).asInstanceOf[Field[R, _]]
      this._fields :+= field
      if (field.unique_?) UNIQUE(field)
      recordFields find (_.name == getter.getName) foreach {recField => this._fieldToRecField += (field -> recField)}
    } else if (classOf[Association[R, _]].isAssignableFrom(cl)) {
      val assoc = getter.invoke(this).asInstanceOf[Association[R, _]]
      this._associations :+= assoc
      this._constraints :+= associationFK(assoc)
      this._fields :+= assoc.field
      if (assoc.unique_?) UNIQUE(assoc.field)
      recordFields find (_.name == getter.getName) foreach {recField => this._fieldToRecField += (assoc.field -> recField)}
    } else if (classOf[Constraint[R]].isAssignableFrom(cl)) {
      val c = getter.invoke(this).asInstanceOf[Constraint[R]]
      this._constraints :+= c
    } else if (classOf[Index[R]].isAssignableFrom(cl)) {
      val i = getter.invoke(this).asInstanceOf[Index[R]]
      this._indexes :+= i
    }
  }


  private def associationFK(a: Association[R, _]): ForeignKey[R] = {
    CONSTRAINT(relationName + "_" + a.name + "_fkey")
    .FOREIGN_KEY(a.field)
    .REFERENCES(a.foreignRelation, a.foreignRelation.PRIMARY_KEY)
    .ON_DELETE(a.onDelete)
    .ON_UPDATE(a.onUpdate)
  }

  protected[orm] def init {
    if (!_initialized) this.synchronized {
      if (!_initialized) try {
        findMembers(this.getClass)
        ORM.dialect.initializeRelation(this)
        this._fields foreach ORM.dialect.initializeField
        this._initialized = true
      } catch {
        case e: NullPointerException =>
          throw new ORMException("Failed to initialize " + relationName + ": " +
                                 "possible cyclic dependency between relations. " +
                                 "Make sure that at least one side uses weak reference to another " +
                                 "(change `val` to `lazy val` for fields and to `def` for inverse associations).", e)
        case e: Exception =>
          throw new ORMException("Failed to initialize " + relationName + ".", e)
      }
    }
  }

  // ### Simple queries

  /**
   * Create `Criteria` for this relation, assigning default `root` alias to it's root node.
   */
  def criteria = as("root").criteria

  /**
   * Retrieve the record by specified `id` from transaction-scoped cache,
   * or fetch it from database.
   */
  def get(id: Long): Option[R] = tx.getCachedRecord(this, id) match {
    case Some(record: R) => Some(record)
    case None => this.criteria.add(this.id EQ id).unique
  }

  /**
   * Fetch all records.
   */
  def all(limit: Int = -1, offset: Int = 0): Seq[R] = {
    this.criteria.limit(limit).offset(offset).list
  }

  // ### Definitions

  protected def CONSTRAINT(name: String): ConstraintHelper[R] = new ConstraintHelper(this, name)
  protected def UNIQUE(fields: Field[R, _]*) =
    CONSTRAINT(relationName + "_" + fields.map(_.name).mkString("_") + "_key").UNIQUE(fields: _*)

  /**
   * Inverse associations.
   */
  def inverse[C](association: Association[C, R]): InverseAssociation[R, C] =
    new InverseAssociation[R, C](association)

  /**
   * Add specified `objects` to this relation's `preAux` queue.
   */
  def addPreAux(objects: SchemaObject*): this.type = {
    _preAuxes ++= (objects filter (!_preAuxes.contains(_)))
    this
  }

  /**
   * Add specified `objects` to this relaion's `postAux` queue.
   */
  def addPostAux(objects: SchemaObject*): this.type = {
    _postAuxes ++= (objects filter (!_postAuxes.contains(_)))
    this
  }

  // ### Persistence

  /**
   * A helper to set parameters to `PreparedStatement`.
   */
  protected[orm] def setParams(record: R, st: PreparedStatement, fields: Seq[Field[R, _]]) = {
    var i = 0
    for (field <- fields) {
      ORM.typeConverter.write(st, field.getValue(record), i + 1)
      i += 1
    }
  }

  /**
   * Uses last generated identity to refetch specified `record`.
   *
   * This method must be called immediately after `insert_!`.
   */
  def refetchLast(record: R) {
    val root = this
    SELECT (root.*) FROM root WHERE (ORM.dialect.lastIdExpression(root)) unique match {
      case Some(r: R) => copyFields(r, record)
      case _ => throw new ORMException("Could not locate the last inserted row.")
    }
  }

  def refetchLast(record: R, latestId: Long) {
    val root = this
    val latestIdEq = if (latestId != -1)
      root.alias + "." + PRIMARY_KEY.name + " = " + latestId
    else ORM.dialect.lastIdExpression(root)
    
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

  def validate(records: Array[R]) {
    var i = 0
    while (i < records.length) {
      validate(records(i))
      i += 1
    }
  }

  /**
   * Skips the validation and performs `INSERT` statement for this record.
   * If no `fields` specified, performs full insert (except empty fields
   * with default values), otherwise only specified `fields` participate
   * in the statement.
   */
  def insert_!(record: R, fields: Field[R, _]*): Int = {
    if (readOnly_?)
      throw new ORMException("The relation " + qualifiedName + " is read-only.")
    else {
      ORM.transactionManager.dml{conn =>
        val fs: Seq[Field[R, _]] = if (fields.isEmpty) this.fields.filter(!_.null_?(record)) else fields
        val sql = ORM.dialect.insertRecord(this, fs)
        sqlLog.debug(sql)
        auto(conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){st =>
          setParams(record, st, fs)
          val rows = st.executeUpdate
          val keys = st.getGeneratedKeys
          if (rows > 0 && keys.next) {
            val latestId = keys.getLong(1)
            // refresh latestId for this record
            updateCache(latestId, record)
          }
          rows
        }
      }
    }
  }
  def INSERT_!(record: R, fields: Field[R, _]*): Int = insert_!(record, fields: _*)

  /**
   * Validates record and executes `insert_!` on success.
   */
  def insert(record: R, fields: Field[R, _]*): Int = validate(record) match {
    case None => insert_!(record, fields: _*)
    case Some(errors) => throw new ValidationException(errors: _*)
  }
  def INSERT(record: R, fields: Field[R, _]*) = insert(record, fields: _*)

  def insertBatch_!(records: Array[R]): Int = {
    if (readOnly_?)
      throw new ORMException("The relation " + qualifiedName + " is read-only.")
    else {
      if (records.length == 0) return 0
      ORM.transactionManager.dml{conn =>
        val fs = this.fields//.filter(_ != id)
        val sql = ORM.dialect.insertRecord(this, fs)
        sqlLog.debug(sql)
        auto(conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){st =>
          var i = 0
          while (i < records.length) {
            setParams(records(i), st, fs)
            st.addBatch
            i += 1
          }

          val rows = st.executeBatch
          val keys = st.getGeneratedKeys
          var count = 0
          // RETURN_GENERATED_KEYS returns only one id (the first or the last)
          var keyId = if (keys.next) keys.getLong(1) else -1
          val idIsOfTheLastRecord = ORM.dialect.returnGeneratedKeysIsTheLast
          var j = if (idIsOfTheLastRecord) rows.length - 1 else 0
          while (j >= 0 && j < rows.length) {
            if (rows(j) > 0) {
              count += 1
              if (keyId > 0) {
                // refresh id for this record
                updateCache(keyId, records(j))
                if (idIsOfTheLastRecord) keyId -= 1 else keyId += 1
              }
            }
            if (idIsOfTheLastRecord) j -= 1 else j += 1
          }
          count
        }
      }
    }
  }

  def insertBatch(records: Array[R]): Int = {
    validate(records)
    insertBatch_!(records)
  }

  /**
   * Skips the validation and performs `UPDATE` statement for this record.
   * If no `fields` specified, performs full update, otherwise only specified
   * `fields` participate in the statement.
   */
  def update_!(record: R, fields: Field[R, _]*): Int = {
    if (readOnly_?)
      throw new ORMException("The relation " + qualifiedName + " is read-only.")
    else {
      ORM.transactionManager.dml{conn =>
        val fs: Seq[Field[R, _]] = if (fields.isEmpty) this.fields/* .filter(_ != id) */ else fields
        val sql = ORM.dialect.updateRecord(this, fs)
        sqlLog.debug(sql)
        auto(conn.prepareStatement(sql)){st =>
          setParams(record, st, fs)
          ORM.typeConverter.write(st, idOf(record), fs.size + 1)
          st.executeUpdate
        }
      }
    }
  }
  def UPDATE_!(record: R, fields: Field[R, _]*): Int = update_!(record, fields: _*)

  /**
   * Validates record and executes `update_!` on success.
   */
  def update(record: R, fields: Field[R, _]*): Int = validate(record) match {
    case None => update_!(record, fields: _*)
    case Some(errors) => throw new ValidationException(errors: _*)
  }
  def UPDATE(record: R, fields: Field[R, _]*) = update(record, fields: _*)

  def updateBatch_!(records: Array[R], fields: Field[R, _]*): Int = {
    if (readOnly_?)
      throw new ORMException("The relation " + qualifiedName + " is read-only.")
    else {
      if (records.length == 0) return 0
      ORM.transactionManager.dml{conn =>
        val fs: Seq[Field[R, _]] = if (fields.isEmpty) this.fields/* .filter(_ != id) */ else fields
        val sql = ORM.dialect.updateRecord(this, fs)
        sqlLog.debug(sql)
        auto(conn.prepareStatement(sql)){st =>
          val paramIdx = fs.size + 1
          var i = 0
          while (i < records.length) {
            val record = records(i)
            setParams(record, st, fs)
            ORM.typeConverter.write(st, idOf(record), paramIdx)
            st.addBatch
            i += 1
          }
        
          val rows = st.executeBatch
          var count = 0
          var j = 0
          while (j < rows.length) {
            if (rows(j) > 0) {
              count += 1
            }
            j += 1
          }
          count
        }
      }
    }
  }

  def updateBatch(records: Array[R], fields: Field[R, _]*): Int = {
    validate(records)
    updateBatch_!(records, fields: _*)
  }

  /**
   * Executes the `DELETE` statement for this record using primary key
   * as delete criteria.
   */
  def delete_!(record: R): Int = {
    if (readOnly_?)
      throw new ORMException("The relation " + qualifiedName + " is read-only.")
    else {
      ORM.transactionManager.dml{conn =>
        val sql = ORM.dialect.deleteRecord(this)
        sqlLog.debug(sql)
        auto(conn.prepareStatement(sql)){st =>
          ORM.typeConverter.write(st, idOf(record), 1)
          st.executeUpdate
        }
      }
    }
  }
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
    val root = this
    val id = idOf(record).get
    SELECT (root.*) FROM root WHERE (root.id EQ id) unique match {
      case Some(r: R) => copyFields(r, record)
      case _ =>
        throw new ORMException("Could not locate record with id = " + id + " in database.")
    }
    return this
  }

  def exists: Boolean = {
    val sql = "select * from " + qualifiedName + " where 1 = 0"
    sqlLog.debug(sql)
    try {
      ORM.transactionManager.sql(sql){st =>
        try {
          val rs = st.executeQuery
          rs.close
          true
        } catch {case _ => false}
      }
    } catch {case _ => false}
  }


  override def equals(that: Any) = that match {
    case r: Relation[R] => r.relationName.equalsIgnoreCase(this.relationName)
    case _ => false
  }

  override def hashCode = this.relationName.toLowerCase.hashCode

  override def toString = qualifiedName

}

// ## Table

abstract class Table[R: Manifest] extends Relation[R] with SchemaObject {
  val objectName = "TABLE " + qualifiedName
  def sqlDrop = {
    init
    ORM.dialect.dropTable(this)
  }
  
  def sqlCreate = {
    init
    ORM.dialect.createTable(this)
  }
}

// ## View

abstract class View[R: Manifest] extends Relation[R] with SchemaObject {

  // ### Miscellaneous

  val objectName = "VIEW " + qualifiedName
  def sqlDrop = {
    init
    ORM.dialect.dropView(this)
  }

  def sqlCreate = {
    init
    ORM.dialect.createView(this)
  }

  /**
   * Views are not updatable by default.
   */
  override def readOnly_?() = true

  /**
   * A `query` that makes up this view definition.
   */
  def query: Select[_]

}
