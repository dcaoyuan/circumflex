package ru.circumflex.orm

class PostgreSQLDialect extends Dialect

class MySQLDialect extends Dialect {
  override def textType = "VARCHAR(4096)"
  override def timestampType = "TIMESTAMP"
  override def supportsSchema_?(): Boolean = false
  override def supportsDropConstraints_?(): Boolean = false
  override def relationQualifiedName(relation: Relation[_]) = relation.relationName
  override def primaryKeyExpression(relation: Relation[_]) = "AUTO_INCREMENT"
  override def initializeRelation(relation: Relation[_]) = {}
  override def lastIdExpression(node: RelationNode[_]) =
    node.alias + "." + node.relation.primaryKey.name + " = LAST_INSERT_ID()"
}

class OracleDialect extends Dialect {
  override def textType = "VARCHAR2(4096)"
  override def timestampType = "TIMESTAMP WITH TIMEZONE"
  override def primaryKeyExpression(relation: Relation[_]) = ""
  override def initializeRelation(relation: Relation[_]) = {
    val seqName = pkSequenceName(relation)
    val seq = new SchemaObject {
      val objectName = "SEQUENCE " + seqName
      val sqlDrop = "DROP SEQUENCE " + seqName
      val sqlCreate = "CREATE SEQUENCE " + seqName
    }
    val trigName = relation.relationName + "_id_auto"
    val trig = new SchemaObject() {
      def objectName = "TRIGGER " + trigName
      def sqlDrop = "DROP TRIGGER " + trigName
      def sqlCreate = "CREATE TRIGGER " + trigName +
      " BEFORE INSERT ON " + relation.qualifiedName + " FOR EACH ROW BEGIN\n" +
      "IF :new." + relation.primaryKey.name + " IS NULL THEN\n\t" +
      "SELECT " + seqName + ".NEXTVAL INTO NEW." + relation.primaryKey.name +
      " FROM " + relation.qualifiedName + ";\n" +
      "END IF;\nEND;"
    }
    relation.addPreAux(seq)
    relation.addPostAux(trig)
  }
}

class H2Dialect extends Dialect {
  override def textType = "VARCHAR(4096)"
  override def timestampType = "TIMESTAMP"
  override def primaryKeyExpression(relation: Relation[_]) = "AUTO_INCREMENT"
  override def floatType (precision: Int = -1, scale: Int = 0) = "FLOAT"
  override def doubleType(precision: Int = -1, scale: Int = 0) = "DOUBLE"
  override def createIndex(idx: Index): String = {
    "CREATE " + (if (idx.unique_?) "UNIQUE " else "") +
    "INDEX " + idx.name + " ON " + idx.relation.qualifiedName + " (" + idx.expression + ")"
  }
}

