package ru.circumflex.orm

// ## SQL dialect

/**
 * A default dialect singleton.
 *
 * If you feel that some of the statements do not work
 * with your RDBMS vendor, trace the exact method and provide it's
 * implementation in your own class. After that, set the `orm.dialect`
 * configuration parameter accordingly.
 */

import java.util.logging.Logger

object DefaultDialect extends Dialect

/**
 * This little thingy does all dirty SQL rendering. We are orienting the default
 * dialect on the world's most advanced open-source database, [PostgreSQL][psql].
 *
 *   [psql]: http://postgresql.org
 */
class Dialect {
  private val log = Logger.getLogger(this.getClass.getName)
  
  // ### SQL types

  def longType = "BIGINT"
  def tinyintType = "TINYINT"
  def integerType = "INTEGER"
  def floatType  (precision: Int = -1, scale: Int = 0) = "FLOAT"   + (if (precision == -1) "" else "(" + precision + "," + scale + ")")
  def doubleType (precision: Int = -1, scale: Int = 0) = "DOUBLE"  + (if (precision == -1) "" else "(" + precision + "," + scale + ")")
  def numericType(precision: Int = -1, scale: Int = 0) = "NUMERIC" + (if (precision == -1) "" else "(" + precision + "," + scale + ")")
  def decimalType(precision: Int = -1, scale: Int = 0) = "DECIMAL" + (if (precision == -1) "" else "(" + precision + "," + scale + ")")
  def textType = "TEXT"
  def varcharType  (length: Int = -1) = "VARCHAR"   + (if (length == -1) "" else "(" + length + ")")
  def varbinaryType(length: Int = -1) = "VARBINARY" + (if (length == -1) "" else "(" + length + ")")
  def booleanType = "BOOLEAN"
  def dateType = "DATE"
  def timeType = "TIME"
  def timestampType = "TIMESTAMPTZ"
  def xmlType = "XML"

  // ### Actions for foreign keys

  def fkNoAction = "NO ACTION"
  def fkCascade = "CASCADE"
  def fkRestrict = "RESTRICT"
  def fkSetNull = "SET NULL"
  def fkSetDefault = "SET DEFAULT"

  // ### Join keywords

  def innerJoin = "INNER JOIN"
  def leftJoin = "LEFT JOIN"
  def rightJoin = "RIGHT JOIN"
  def fullJoin = "FULL JOIN"

  // ### Predicates

  def EQ = "= ?"
  def NE = "<> ?"
  def GT = "> ?"
  def GE = ">= ?"
  def LT = "< ?"
  def LE = "<= ?"

  def emptyPredicate = "1 = 1"
  def isNull = "IS NULL"
  def isNotNull = "IS NOT NULL"
  def like = "LIKE ?"
  def ilike = "ILIKE ?"
  def between = "BETWEEN ? AND ?"
  def in = "IN"
  def notIn = "NOT IN"
  def parameterizedIn(params: Any*) = "IN (" + params.map{p => "?"}.mkString(", ") + ")"

  def and = "AND"
  def or = "OR"
  def not = "NOT"

  def all = "ALL"
  def some = "SOME"

  def exists = "EXISTS"
  def notExists = "NOT EXISTS"

  // ### Functions and others

  def NULL = "NULL"
  def distinct = "DISTINCT"
  def count = "COUNT"
  def max = "MAX"
  def min = "MIN"
  def sum = "SUM"
  def avg = "AVG"
  def bitAnd(expr1: String, expr2: Any) = "BITAND(" + expr1 + "," + expr2 + ")"

  // ### Set operations

  def union = "UNION"
  def unionAll = "UNION ALL"
  def except = "EXCEPT"
  def exceptAll = "EXCEPT ALL"
  def intersect = "INTERSECT"
  def intersectAll = "INTERSECT ALL"

  // ### Order specificators

  def asc = "ASC"
  def desc = "DESC"

  // ### Features compliance

  def supportsSchema_?(): Boolean = true
  def supportsDropConstraints_?(): Boolean = true

  // ### Commons

  /**
   * Quote literal expression as described in SQL92 standard.
   */
  def quoteLiteral(expr: String) = "'" + expr.replace("'", "''") + "'"

  /**
   * Quotes identifier for dialects that support it.
   */
  def quoteIdentifer(identifier: String) = identifier

  /**
   * Qualify relation name with it's schema.
   */
  def relationQualifiedName(relation: Relation[_]) =
    relation.schema.name + "." + relation.relationName

  /**
   * Just append `AS` and specified `alias` to specified `expression`.
   */
  def alias(expression: String, alias: String) = expression + " AS " + alias

  /**
   * Qualify a column with table alias (e.g. "p.id")
   */
  def qualifyColumn(field: Field[_, _], tableAlias: String) =
    tableAlias + "." + field.name

  /**
   * Take specified `expression` into parentheses and prepend `ON`.
   */
  def on(expression: String) = "ON (" + expression + ")"

  /**
   * Take specified `expression` in parentheses and prepend `NOT`.
   */
  def not(expression: String) = "NOT (" + expression + ")"

  /**
   * Take specified `subquery` into parentheses and prepend with
   * specified `expression`.
   */
  def subquery(expression: String, subquery: SQLQuery[_]) =
    expression + " ( " + subquery.toSql + " )"

  // ### DDL

  /**
   * Produce a full definition of constraint (prepends the specific definition
   * with `CONSTRAINT` keyword and constraint name.
   */
  def constraintDefinition(constraint: Constraint[_]) =
    "CONSTRAINT " + constraint.constraintName + " " + constraint.sqlDefinition

  /**
   * Produce `ALTER TABLE` statement with abstract action.
   */
  def alterTable(rel: Relation[_], action: String) =
    "ALTER TABLE " + rel.qualifiedName + " " + action

  /**
   * Produce `ALTER TABLE` statement with `ADD CONSTRAINT` action.
   */
  def alterTableAddConstraint(constraint: Constraint[_]) =
    alterTable(constraint.relation, "ADD " + constraintDefinition(constraint));

  /**
   * Produce `ALTER TABLE` statement with `DROP CONSTRAINT` action.
   */
  def alterTableDropConstraint(constraint: Constraint[_]) =
    alterTable(constraint.relation, "DROP CONSTRAINT " + constraint.constraintName);

  /**
   * Produce `SET REFERENTIAL_INTEGRITY` TRUE/FALSE.
   */
  def setReferentialIntegrity(enable: Boolean) =
    "SET REFERENTIAL_INTEGRITY " + enable

  /**
   * Produces `CREATE SCHEMA` statement.
   */
  def createSchema(schema: Schema) = "CREATE SCHEMA " + schema.name

  /**
   * Produce `DROP SCHEMA` statement.
   */
  def dropSchema(schema: Schema) = "DROP SCHEMA " + schema.name + " CASCADE"

  /**
   * Produce `CREATE TABLE` statement without constraints.
   */
  def createTable(table: Table[_]) = (
    "CREATE TABLE " + table.qualifiedName + " (" +
    table.fields.map(_.toSql).mkString(", ") +
    ", PRIMARY KEY (" + table.PRIMARY_KEY.name + "))"
  )
  /**
   * Produce `DROP TABLE` statement.
   */
  def dropTable(table: Table[_]) =
    "DROP TABLE " + table.qualifiedName

  /**
   * Produces `CREATE VIEW` statement.
   */
  def createView(view: View[_]) =
    "CREATE VIEW " + view.qualifiedName + " (" +
  view.fields.map(_.name).mkString(", ") + ") AS " +
  view.query.toInlineSql

  /**
   * Produce `DROP VIEW` statement.
   */
  def dropView(view: View[_]) =
    "DROP VIEW " + view.qualifiedName

  /**
   * Produce `CREATE INDEX` statement.
   */
  def createIndex(idx: Index[_]): String = {
    "CREATE " + (if (idx.unique_?) "UNIQUE " else "") +
    "INDEX " + idx.name + " ON " + idx.relation.qualifiedName + " (" + idx.expression + ")" +
    " USING " + idx.using +
    (if (idx.where != EmptyPredicate) " WHERE " + idx.where.toInlineSql else "")
  }

  /**
   * Produce `DROP INDEX` statement.
   */
  def dropIndex(idx: Index[_]) = "DROP INDEX " + idx.name + " ON " + idx.relation.qualifiedName

  def createSequence(seq: Sequence) =
    "CREATE SEQUENCE " + seq.name

  def dropSequence(seq: Sequence) =
    "DROP SEQUENCE " + seq.name

  /**
   * SQL definition for a column represented by specified `field`
   * (e.g. `mycolumn VARCHAR NOT NULL`).
   */
  def columnDefinition(field: Field[_, _]): String = {
    var result = field.name + " " + field.sqlType
    if (field.notNull_?) result += " NOT NULL"
    result += defaultExpression(field)
    return result
  }

  /**
   * Make necessary stuff for relation initialization.
   *
   * This implementation adds an auxiliary sequence for primary key
   * (sequences are supported by PostgreSQL, Oracle and DB2 dialects).
   */
  def initializeRelation(relation: Relation[_]): Unit = {}

  /**
   * Performs dialect-specific field initialization.
   */
  def initializeField(field: Field[_, _]) {
    field match {
      case f: AutoIncrementable[_, _] if f.autoIncrement_? && !field.relation.isInstanceOf[View[_]] =>
        val seqName = sequenceName(f)
        val seq = new Sequence(seqName)
        f.relation.addPreAux(seq)
      case _ =>
    }
  }

  /**
   * Produces a `DEFAULT` expression for specified `field`.
   */
  def defaultExpression(field: Field[_, _]): String = field match {
    case a: AutoIncrementable[_, _] if a.autoIncrement_? => " DEFAULT NEXTVAL('" + sequenceName(field) + "')"
    case _ => field.defaultExpression.map(" DEFAULT " + _).getOrElse("")
  }

  protected def sequenceName(f: Field[_, _]): String = {
    quoteIdentifer(f.relation.schema.name) + "." +
    quoteIdentifer(f.relation.relationName + "_" + f.name + "_seq")
  }
  
  /**
   * Produce unique constraint definition (e.g. `UNIQUE (name, value)`).
   */
  def uniqueKeyDefinition(uniq: UniqueKey[_]) =
    "UNIQUE (" + uniq.fields.map(_.name).mkString(", ") + ")"

  /**
   * Produce foreign key constraint definition for association (e.g.
   * `FOREIGN KEY (country_id) REFERENCES country(id) ON DELETE CASCADE`).
   */
  def foreignKeyDefinition(fk: ForeignKey[_]) = (
    "FOREIGN KEY (" + fk.localFields.map(_.name).mkString(", ") +
    ") REFERENCES " + fk.foreignRelation.qualifiedName + " (" +
    fk.foreignFields.map(_.name).mkString(", ") + ") " +
    "ON DELETE " + fk.onDelete.toSql + " " +
    "ON UPDATE " + fk.onUpdate.toSql
  )
  /**
   * Produces check constraint definition (e.g. `CHECK (index > 0)`).
   */
  def checkConstraintDefinition(check: CheckConstraint[_]) =
    "CHECK (" + check.expression + ")"

  // ### SQL

  def lastIdExpression(node: RelationNode[_]) =
    node.alias + "." + node.relation.PRIMARY_KEY.name + " = LASTVAL()"

  /**
   * Produce SQL representation of joined tree of relations (`JoinNode` instance).
   */
  def join(j: JoinNode[_, _]): String = joinInternal(j, null)

  /**
   * Some magic to convert join tree to SQL.
   */
  protected def joinInternal(node: RelationNode[_], on: String): String = {
    var result = ""
    node match {
      case j: JoinNode[_, _] =>
        result += joinInternal(j.left, on) +
        " " + j.joinType.toSql + " " +
        joinInternal(j.right, j.sqlOn)
      case _ =>
        result += node.toSql
        if (on != null) result += " " + on
    }
    return result
  }

  /**
   * Produces `SELECT` statement (without `LIMIT`, `OFFSET` and `ORDER BY`
   * clauses).
   */
  def select(q: Select[_]): String = {
    var result = "SELECT " + q.projections.map(_.toSql).mkString(", ")
    if (q.from.size > 0)
      result += " FROM " + q.from.map(_.toSql).mkString(", ")
    if (q.where != EmptyPredicate)
      result += " WHERE " + q.where.toSql
    if (q.groupBy.size > 0)
      result += " GROUP BY " + q.groupBy.flatMap(_.sqlAliases).mkString(", ")
    if (q.having != EmptyPredicate)
      result += " HAVING " + q.having.toSql
    q.setOps.foreach {
      case (op: SetOperation, subq: SQLQuery[_]) =>
        result += " " + op.toSql + " ( " + subq.toSql + " )"
      case _ =>
    }
    if (q.orderBy.size > 0)
      result += " ORDER BY " + q.orderBy.map(_.toSql).mkString(", ")
    if (q.limit > -1)
      result += " LIMIT " + q.limit
    if (q.offset > 0)
      result += " OFFSET " + q.offset
    return result
  }

  /**
   * Returns a predicate expression for querying the last inserted record
   * for `IdentityGenerator`.
   */
  def identityLastIdPredicate[T](node: RelationNode[T]): Predicate =
    new SimpleExpression(node.alias + "." + node.relation.PRIMARY_KEY.name + " = LASTVAL()", Nil)

  /**
   * Returns a query which retrieves the last generated identity value for `IdentityGenerator`.
   */
  def identityLastIdQuery[T](node: RelationNode[T]): SQLQuery[T] =
    new Select(expr[T]("LASTVAL()"))

  /**
   * Returns a query which retrieves the next sequence value for the primary key of specified `node`.
   */
  def sequenceNextValQuery[T](node: RelationNode[T]): SQLQuery[T] =
    new Select(expr[T]("NEXTVAL('" + sequenceName(node.relation.PRIMARY_KEY) + "')"))
  
  /*!## Data Manipulation Language */

  /**
   * Produce `INSERT INTO .. VALUES` statement for specified `record` and specified `fields`.
   */
  def insertRecord(relation: Relation[_], fields: Seq[Field[_, _]]) =
    "INSERT INTO " + relation.qualifiedName +
  " (" + fields.map(_.name).mkString(", ") +
  ") VALUES (" + fields.map(f => "?").mkString(", ") + ")"

  /**
   * Produce `UPDATE` statement with primary key criteria for specified `record` using specified
   * `fields` in the `SET` clause.
   */
  def updateRecord(relation: Relation[_], fields: Seq[Field[_, _]]): String =
    "UPDATE " + relation.qualifiedName +
  " SET " + fields.map(_.name + " = ?").mkString(", ") +
  " WHERE " + relation.id.name + " = ?"

  /**
   * Produce `DELETE` statement for specified `record`.
   */
  def deleteRecord(relation: Relation[_]): String =
    "DELETE FROM " + relation.qualifiedName +
  " WHERE " + relation.id.name + " = ?"

  /**
   * Produce `INSERT .. SELECT` statement.
   */
  def insertSelect(dml: InsertSelect[_]) = "INSERT INTO " + dml.relation.qualifiedName + " (" +
  dml.relation.fields.map(_.name).mkString(", ") + ") " + dml.query.toSql

  /**
   * Produce `UPDATE` statement.
   */
  def update(dml: Update[_]): String = {
    var result = "UPDATE " + dml.node.toSql + " SET " +
    dml.setClause.map(_._1.name + " = ?").mkString(", ")
    if (dml.where != EmptyPredicate) result += " WHERE " + dml.where.toSql
    return result
  }

  /**
   * Produce `DELETE` statement.
   */
  def delete(dml: Delete[_]): String = {
    var result = "DELETE FROM " + dml.node.toSql
    if (dml.where != EmptyPredicate) result += " WHERE " + dml.where.toSql
    return result
  }

  /**
   * @return true  if the last  id
   *         false if the first id
   */
  def returnGeneratedKeysIsTheLast = true
}
