package ru.circumflex.orm

class PostgreSQLDialect extends Dialect {
  override def timestampType = "TIMESTAMPTZ"
}

class MySQLDialect extends Dialect {
  override def textType = "VARCHAR(4096)"
  override def timestampType = "TIMESTAMP"
  override def supportsSchema_?(): Boolean = false
  override def supportsDropConstraints_?(): Boolean = false
  override def relationQualifiedName(relation: Relation[_]) = relation.relationName

  // do nothing -- for MySQL you don't need to create manually a sequence for auto-incrementable fields
  override def initializeField(field: Field[_, _]) {}

  override def defaultExpression(field: Field[_, _]): String = field match {
    case a: AutoIncrementable[_, _] if a.autoIncrement_? => " AUTO_INCREMENT"
    case _ => field.defaultExpression.map(" DEFAULT " + _).getOrElse("")
  }

  override def identityLastIdPredicate[T](node: RelationNode[T]): Predicate =
    new SimpleExpression(node.alias + "." + node.relation.PRIMARY_KEY.name + " = LAST_INSERT_ID()", Nil)

  override def identityLastIdQuery[T](node: RelationNode[T]): SQLQuery[T] =
    new Select(expr[T]("LAST_INSERT_ID()"))

  override def sequenceNextValQuery[T](node: RelationNode[T]): SQLQuery[T] =
    throw new UnsupportedOperationException("This operation is unsupported in the MySQL dialect.")

  override def lastIdExpression(node: RelationNode[_]) =
    node.alias + "." + node.relation.PRIMARY_KEY.name + " = LAST_INSERT_ID()"
  override def setReferentialIntegrity(enable: Boolean) =
    "set foreign_key_checks = " + (if (enable) "1" else "0")
  override def bitAnd(expr1: String, expr2: Any) = "(" + expr1 + " & " + expr2 + ")"
  override def returnGeneratedKeysIsTheLast = false
}

class OracleDialect extends Dialect {
  override def tinyintType = "NUMBER(1)"
  override def textType = "VARCHAR2(4096)"
  override def timestampType = "TIMESTAMP WITH TIMEZONE"

  override def initializeField(field: Field[_, _]) {
    field match {
      case f: AutoIncrementable[_, _] if f.autoIncrement_? && !field.relation.isInstanceOf[View[_]] =>
        val seqName = sequenceName(f)
        val seq = new Sequence(seqName)
        f.relation.addPreAux(seq)
        
        val trigName = f.relation.relationName + "_id_auto"
        val trig = new SchemaObject() {
          def objectName = "TRIGGER " + trigName
          def sqlDrop = "DROP TRIGGER " + trigName
          def sqlCreate = "CREATE TRIGGER " + trigName +
          " BEFORE INSERT ON " + f.relation.qualifiedName + " FOR EACH ROW BEGIN\n" +
          "IF :new." + f.relation.PRIMARY_KEY.name + " IS NULL THEN\n\t" +
          "SELECT " + seqName + ".NEXTVAL INTO NEW." + f.relation.PRIMARY_KEY.name +
          " FROM " + f.relation.qualifiedName + ";\n" +
          "END IF;\nEND;"
        }
        f.relation.addPostAux(trig)
      case _ =>
    }
  }

  override def defaultExpression(field: Field[_, _]): String = field match {
    case a: AutoIncrementable[_, _] if a.autoIncrement_? => ""
    case _ => field.defaultExpression.map(" DEFAULT " + _).getOrElse("")
  }

  // @todo
  override def returnGeneratedKeysIsTheLast = false
}

class H2Dialect extends Dialect {
  override def textType = "VARCHAR(4096)"
  override def timestampType = "TIMESTAMP"
  override def floatType (precision: Int = -1, scale: Int = 0) = "REAL"
  override def doubleType(precision: Int = -1, scale: Int = 0) = "DOUBLE"

  override def defaultExpression(field: Field[_, _]): String = field match {
    case a: AutoIncrementable[_, _] if a.autoIncrement_? => " AUTO_INCREMENT"
    case _ => field.defaultExpression.map(" DEFAULT " + _).getOrElse("")
  }

  override def createIndex(idx: Index[_]): String = {
    "CREATE " + (if (idx.unique_?) "UNIQUE " else "") +
    "INDEX " + idx.name + " ON " + idx.relation.qualifiedName + " (" + idx.expression + ")"
  }
  override protected def sequenceName(field: Field[_, _]) = quoteIdentifer(field.relation.relationName + "_" + field.name + "_seq")
  override def supportsDropConstraints_? = false
  override def dropSchema(schema: Schema) = "DROP SCHEMA IF EXISTS " + schema.name
  override def dropIndex(idx: Index[_]) = "DROP INDEX IF EXISTS " + idx.name
  override def dropTable(table: Table[_]) = "DROP TABLE IF EXISTS " + table.relationName
  override def dropView(view: View[_]) = "DROP VIEW IF EXISTS " + view.relationName
  override def dropSequence(seq: Sequence) = "DROP SEQUENCE IF EXISTS " + seq.name
  override def returnGeneratedKeysIsTheLast = true
}

class DerbyDialect extends Dialect {
  override def textType = "VARCHAR(4096)"
  override def timestampType = "TIMESTAMP"
  override def floatType (precision: Int = -1, scale: Int = 0) = "REAL"
  override def doubleType(precision: Int = -1, scale: Int = 0) = "DOUBLE"

  override def defaultExpression(field: Field[_, _]): String = field match {
    case a: AutoIncrementable[_, _] if a.autoIncrement_? => " AUTO_INCREMENT"
    case _ => field.defaultExpression.map(" DEFAULT " + _).getOrElse("")
  }

  override def createIndex(idx: Index[_]): String = {
    "CREATE " + (if (idx.unique_?) "UNIQUE " else "") +
    "INDEX " + idx.name + " ON " + idx.relation.qualifiedName + " (" + idx.expression + ")"
  }

  override protected def sequenceName(field: Field[_, _]) = quoteIdentifer(field.relation.relationName + "_" + field.name + "_seq")
  override def supportsDropConstraints_? = false
  override def dropSchema(schema: Schema) = "DROP SCHEMA " + schema.name
  override def dropIndex(idx: Index[_]) = "DROP INDEX " + idx.name
  override def dropTable(table: Table[_]) =  "DROP TABLE " + table.relationName
  override def dropView(view: View[_]) = "DROP VIEW " + view.relationName

}

