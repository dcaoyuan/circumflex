package ru.circumflex.orm

import ORM._

// ## Schema Objects for DDL

// ### Schema

class Schema(val name: String) extends SchemaObject {
  def objectName = "SCHEMA " + name
  def sqlCreate = dialect.createSchema(this)
  def sqlDrop = dialect.dropSchema(this)
}

// ### Constraints

/**
 * Common stuff for all constraints.
 */
abstract class Constraint[R](val relation: Relation[R],
                             val constraintName: String)
extends SchemaObject with SQLable {

  val objectName = "CONSTRAINT " + constraintName
  val sqlCreate = dialect.alterTableAddConstraint(this)
  val sqlDrop = dialect.alterTableDropConstraint(this)
  val toSql = dialect.constraintDefinition(this)

  def sqlDefinition: String

  override def toString = toSql
}

/**
 * An SQL `UNIQUE` constraint.
 */
class UniqueKey[R](relation: Relation[R],
                   name: String,
                   val fields: Seq[Field[R, _]]
) extends Constraint(relation, name) {
  def sqlDefinition = dialect.uniqueKeyDefinition(this)
}

/**
 * An SQL `FOREIGN KEY` constraint.
 */
class ForeignKey[R](relation: Relation[R],
                    name: String,
                    val foreignRelation: Relation[_],
                    val localFields: Seq[Field[R, _]],
                    val foreignFields: Seq[Field[_, _]],
                    protected var _onDelete: ForeignKeyAction,
                    protected var _onUpdate: ForeignKeyAction
) extends Constraint(relation, name) {

  def onDelete = _onDelete
  def onDelete(action: ForeignKeyAction): this.type = {
    _onDelete = action
    this
  }
  def ON_DELETE(action: ForeignKeyAction): this.type = onDelete(action)

  def onUpdate = _onUpdate
  def onUpdate(action: ForeignKeyAction): this.type = {
    _onUpdate = action
    this
  }
  def ON_UPDATE(action: ForeignKeyAction): this.type = onUpdate(action)

  def sqlDefinition = dialect.foreignKeyDefinition(this)
}

/**
 * An SQL `FOREIGN KEY` constraint.
 */
class CheckConstraint[R](relation: Relation[R], name: String, val expression: String) extends Constraint(relation, name) {
  def sqlDefinition = dialect.checkConstraintDefinition(this)
}

class Index[R](val relation: Relation[R], val name: String, expressions: String*) extends SchemaObject {

  def expression = expressions.mkString(", ")

  /**
   * DSL for defining `UNIQUE` indexes.
   */
  protected var _unique: Boolean = false
  def unique_?() = _unique
  def unique: this.type = {
    this._unique = true
    return this
  }
  def UNIQUE: this.type = unique

  /**
   * DSL for defining indexing method.
   */
  private var _method: String = "btree"
  def using = _method
  def using(method: String): this.type = {
    this._method = method
    return this
  }
  def USING(method: String): this.type = using(method)

  /**
   * DSL for defining indexing predicate.
   */
  private var _predicate: Predicate = EmptyPredicate
  def where = _predicate
  def where(predicate: Predicate): this.type = {
    this._predicate = predicate
    return this
  }
  def WHERE(predicate: Predicate): this.type = where(predicate)

  val objectName = "INDEX " + name
  val sqlCreate = dialect.createIndex(this)
  val sqlDrop = dialect.dropIndex(this)
}

class Sequence(val name: String) extends SchemaObject {
  val objectName = "SEQUENCE " + name
  val sqlCreate = dialect.createSequence(this)
  val sqlDrop = dialect.dropSequence(this)
}

/**
 * A helper to create SQL constraints for the relation.
 */
class ConstraintHelper[R](relation: Relation[R], name: String) {
  def UNIQUE(fields: Field[R, _]*): UniqueKey[R] = new UniqueKey[R](relation, name, fields.toList)

  def CHECK(expression: String): CheckConstraint[R] = new CheckConstraint[R](relation, name, expression)

  def FOREIGN_KEY(foreignRelation: Relation[R], localFields: Seq[Field[R, _]], foreignFields: Seq[Field[R, _]]): ForeignKey[R] =
    new ForeignKey[R](relation, name, foreignRelation, localFields, foreignFields, NO_ACTION, NO_ACTION)

  def FOREIGN_KEY(foreignRelation: Relation[R], fields: Pair[Field[R, _], Field[R, _]]*): ForeignKey[R] = {
    val localFileds = fields.map(_._1)
    val foreignFields = fields.map(_._2)
    FOREIGN_KEY(foreignRelation, localFileds, foreignFields)
  }

  def FOREIGN_KEY(localFields: Field[R, _]*): ForeignKeyHelper[R] = new ForeignKeyHelper[R](relation, name, localFields)
}

/**
 * A special helper for creating foreign keys in DSL style.
 */
class ForeignKeyHelper[R](relation: Relation[R], name: String, localFields: Seq[Field[R, _]]) {
  def REFERENCES(foreignRelation: Relation[_], foreignFields: Field[_, _]*): ForeignKey[R] =
    new ForeignKey(relation, name, foreignRelation, localFields, foreignFields, NO_ACTION, NO_ACTION)
}

class DefinitionHelper[R](relation: Relation[R], name: String) {
  def TINYINT() = new TinyintField[R](relation, name)
  def INTEGER() = new IntField[R](relation, name)
  def BIGINT() = new LongField[R](relation, name)
  def FLOAT(precision: Int = -1, scale: Int = 1) = new FloatField[R](relation, name, precision, scale)
  def DOUBLE(precision: Int = -1, scale: Int = 1) = new DoubleField[R](relation, name, precision, scale)
  def NUMERIC(precision: Int = -1, scale: Int = 1) = new NumericField[R](relation, name, precision, scale)
  def TEXT() = new TextField[R](relation, name, dialect.textType)
  def VARCHAR(length: Int = -1) = new TextField[R](relation, name, length)
  def VARBINARY(length: Int = -1) = new VarbinaryField[R](relation, name, length)
  def SERIALIZED[T](tpe: Class[T], length: Int = -1) = new SerializedField[R, T](relation, name, tpe, length)
  def BOOLEAN() = new BooleanField[R](relation, name)
  def DATE() = new DateField[R](relation, name)
  def TIME() = new TimeField[R](relation, name)
  def TIMESTAMP() = new TimestampField[R](relation, name)
  def XML() = new XmlField[R](relation, name)

  def INDEX(expression: String) = new Index[R](relation, name, expression)

  //def REFERENCES[F](toRelation: Relation[F]): Association[R, F] = new Association[R, F](relation, name, toRelation)
}

// ### Indexes

