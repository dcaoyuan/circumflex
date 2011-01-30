package ru.circumflex.orm.avro

import java.sql.ResultSet
import ru.circumflex.orm.Relation
import ru.circumflex.orm.RelationNode

/**
 *
 * @author Caoyuan Deng
 */
class AvroNode[R](val relation: Relation[R], fileName: String) extends RelationNode[R] {

  def fromAvro(): ResultSet = {
    Avro().read(fileName, null)
  }
}
