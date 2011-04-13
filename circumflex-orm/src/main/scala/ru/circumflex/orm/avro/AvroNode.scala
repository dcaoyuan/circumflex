package ru.circumflex.orm.avro

import java.io.File
import java.sql.ResultSet
import ru.circumflex.orm.ORM
import ru.circumflex.orm.Relation
import ru.circumflex.orm.RelationNode

/**
 *
 * @author Caoyuan Deng
 */
class AvroNode[R] private (val relation: Relation[R], fileName: String) extends RelationNode[R] {

  def fromAvro(): ResultSet = {
    val filePath = if (fileName == null) ORM.avroDir + File.separator + relation.relationName + ".avro" else fileName
    Avro().read(filePath, null)
  }
}

object AvroNode {
  def apply[R](relation: Relation[R], fileName: String): AvroNode[R] = new AvroNode(relation, fileName)
}
