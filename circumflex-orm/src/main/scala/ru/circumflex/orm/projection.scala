package ru.circumflex.orm

import ORM._
import java.sql.ResultSet

// ## Projection Basics

trait Projection[T] extends SQLable {

  var query: SQLQuery[_] = _
  
  /**
   * Extract a value from result set.
   */
  def read(rs: ResultSet): T

  /**
   * Returns the list of aliases, from which this projection is composed.
   */
  def sqlAliases: Seq[String]

  override def toString = toSql
}

/**
 * A contract for single-column projections with assigned aliases
 * so that they may be used in ORDER BY/HAVING clauses.
 */
trait AtomicProjection[T] extends Projection[T] {
  /**
   * Projection's alias (`this` is expanded to query-unique alias).
   */
  protected[orm] var alias: String = "this"

  def read(rs: ResultSet) = typeConverter.read(rs, alias).asInstanceOf[T]

  /**
   * Change an alias of this projection.
   */
  def as(alias: String): this.type = {
    this.alias = alias
    return this
  }

  def sqlAliases = List(alias)
}

/**
 * A contract for multi-column projections that should be read as a composite object.
 */
trait CompositeProjection[R] extends Projection[R] {

  private var _hash = 0

  val subProjections: Seq[Projection[_]]

  def sqlAliases = subProjections.flatMap(_.sqlAliases)

  override def equals(obj: Any) = obj match {
    case p: CompositeProjection[R] =>
      (this.subProjections.toList -- p.subProjections.toList) == Nil
    case _ => false
  }

  override def hashCode: Int = {
    if (_hash == 0)
      for (p <- subProjections)
        _hash = 31 * _hash + p.hashCode
    return _hash
  }

  def toSql = subProjections.map(_.toSql).mkString(", ")

}

// ## Expression Projection

/**
 * This projection represents an arbitrary expression that RDBMS can understand
 * within `SELECT` clause (for example, `current_timestamp` or `count(*)`).
 */
class ExpressionProjection[T](val expression: String) extends AtomicProjection[T] {

  def toSql = dialect.alias(expression, alias)

  override def equals(obj: Any) = obj match {
    case p: ExpressionProjection[T] =>
      p.expression == this.expression
    case _ => false
  }

  override def hashCode = expression.hashCode
}

// ## Field Projection

/**
 * A projection for single field of a record.
 */
class FieldProjection[T, R <: AnyRef](val node: RelationNode[R],
                                      val field: Field[T]
) extends AtomicProjection[T] {

  /**
   * Returns a column name qualified with node's alias.
   */
  def expr = dialect.qualifyColumn(field, node.alias)

  def toSql = dialect.alias(expr, alias)

  override def equals(obj: Any) = obj match {
    case p: FieldProjection[T, R] =>
      p.node == this.node && p.field.name == this.field.name
    case _ => false
  }

  override def hashCode = node.hashCode * 31 + field.name.hashCode
}

// ## Record Projection

/**
 * A projection for reading entire `Record`.
 */
class RecordProjection[R <:AnyRef](val node: RelationNode[R]) extends CompositeProjection[R] {
  protected val _fieldProjections: Seq[FieldProjection[Any, R]] = (
    node
    .relation
    .fields
    .map(f => new FieldProjection(node, f.asInstanceOf[Field[Any]]))
  )
  
  val subProjections = _fieldProjections

  // We will return this `null`s in any failure conditions.
  protected def nope: R = null.asInstanceOf[R]

  def read(rs: ResultSet): R =
    _fieldProjections.find(_.field == node.relation.primaryKey) match {
      case Some(pkProjection) =>
        pkProjection.read(rs) match {
          case id: Long =>
            // Has this record been cached? if true, should use and update it via rs instead of creae a new instance
            val record: R = node.relation.recordOf(id) match {
              case Some(x) => x
              case None => node.relation.recordClass.newInstance
            }
            readRecord(rs, record)
          case x => throw new Exception("Error in read id of " + node.relation.recordClass.getSimpleName + ", which alias is '" + pkProjection.alias + "', value read is " + x)
        }
      case _ => nope
    }

  protected def readRecord(rs: ResultSet, record: R): R = {
    query.recordsHolder += record
    _fieldProjections foreach {
      case p if p.field == node.relation.primaryKey =>
        node.relation.updateCache(p.read(rs).asInstanceOf[Long], record)
      case p =>
        p.field.setValue(record, p.read(rs).asInstanceOf[AnyRef]) match {
          case Some(lazyAction) => query.lazyFetchers += lazyAction
          case None =>
        }
    }
    // If record remains unidentified, do not return it.
    if (node.relation.transient_?(record)) return nope
    else {
      // Otherwise cache it and return.
      //tx.updateRecordCache(node.relation, record)
      return record
    }
  }

  override def equals(obj: Any) = obj match {
    case p: RecordProjection[R] => this.node == p.node
    case _ => false
  }

  override def hashCode = node.hashCode

}

// ## Projections for tuples

class UntypedTupleProjection(override val subProjections: Projection[_]*) extends CompositeProjection[Array[Any]] {
  def read(rs: ResultSet): Array[Any] = subProjections.map(_.read(rs)).toArray
}

case class Tuple2Projection[T1, T2] (
  val _1: Projection[T1],
  val _2: Projection[T2]
) extends CompositeProjection[Tuple2[T1, T2]] {
  val subProjections = List[Projection[_]](_1, _2)
  def read(rs: ResultSet): Tuple2[T1, T2] =
    (_1.read(rs), _2.read(rs))
}

case class Tuple3Projection[T1, T2, T3] (
  val _1: Projection[T1],
  val _2: Projection[T2],
  val _3: Projection[T3]
) extends CompositeProjection[Tuple3[T1, T2, T3]] {
  val subProjections = List[Projection[_]](_1, _2, _3)
  def read(rs: ResultSet): Tuple3[T1, T2, T3] =
    (_1.read(rs), _2.read(rs), _3.read(rs))
}

case class Tuple4Projection[T1, T2, T3, T4] (
  val _1: Projection[T1],
  val _2: Projection[T2],
  val _3: Projection[T3],
  val _4: Projection[T4]
) extends CompositeProjection[Tuple4[T1, T2, T3, T4]] {
  val subProjections = List[Projection[_]](_1, _2, _3, _4)
  def read(rs: ResultSet): Tuple4[T1, T2, T3, T4] =
    (_1.read(rs), _2.read(rs), _3.read(rs), _4.read(rs))
}

case class Tuple5Projection[T1, T2, T3, T4, T5] (
  val _1: Projection[T1],
  val _2: Projection[T2],
  val _3: Projection[T3],
  val _4: Projection[T4],
  val _5: Projection[T5]
) extends CompositeProjection[Tuple5[T1, T2, T3, T4, T5]] {
  val subProjections = List[Projection[_]](_1, _2, _3, _4, _5)
  def read(rs: ResultSet): Tuple5[T1, T2, T3, T4, T5] =
    (_1.read(rs), _2.read(rs), _3.read(rs), _4.read(rs), _5.read(rs))
}

case class Tuple6Projection[T1, T2, T3, T4, T5, T6] (
  val _1: Projection[T1],
  val _2: Projection[T2],
  val _3: Projection[T3],
  val _4: Projection[T4],
  val _5: Projection[T5],
  val _6: Projection[T6]
) extends CompositeProjection[Tuple6[T1, T2, T3, T4, T5, T6]] {
  val subProjections = List[Projection[_]](_1, _2, _3, _4, _5, _6)
  def read(rs: ResultSet): Tuple6[T1, T2, T3, T4, T5, T6] =
    (_1.read(rs), _2.read(rs), _3.read(rs), _4.read(rs), _5.read(rs),
     _6.read(rs))
}

case class Tuple7Projection[T1, T2, T3, T4, T5, T6, T7] (
  val _1: Projection[T1],
  val _2: Projection[T2],
  val _3: Projection[T3],
  val _4: Projection[T4],
  val _5: Projection[T5],
  val _6: Projection[T6],
  val _7: Projection[T7]
) extends CompositeProjection[Tuple7[T1, T2, T3, T4, T5, T6, T7]] {
  val subProjections = List[Projection[_]](_1, _2, _3, _4, _5, _6, _7)
  def read(rs: ResultSet): Tuple7[T1, T2, T3, T4, T5, T6, T7] =
    (_1.read(rs), _2.read(rs), _3.read(rs), _4.read(rs), _5.read(rs),
     _6.read(rs), _7.read(rs))
}

case class Tuple8Projection[T1, T2, T3, T4, T5, T6, T7, T8] (
  val _1: Projection[T1],
  val _2: Projection[T2],
  val _3: Projection[T3],
  val _4: Projection[T4],
  val _5: Projection[T5],
  val _6: Projection[T6],
  val _7: Projection[T7],
  val _8: Projection[T8]
) extends CompositeProjection[Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]] {
  val subProjections = List[Projection[_]](_1, _2, _3, _4, _5, _6, _7, _8)
  def read(rs: ResultSet): Tuple8[T1, T2, T3, T4, T5, T6, T7, T8] =
    (_1.read(rs), _2.read(rs), _3.read(rs), _4.read(rs), _5.read(rs),
     _6.read(rs), _7.read(rs), _8.read(rs))
}

case class Tuple9Projection[T1, T2, T3, T4, T5, T6, T7, T8, T9] (
  val _1: Projection[T1],
  val _2: Projection[T2],
  val _3: Projection[T3],
  val _4: Projection[T4],
  val _5: Projection[T5],
  val _6: Projection[T6],
  val _7: Projection[T7],
  val _8: Projection[T8],
  val _9: Projection[T9]
) extends CompositeProjection[Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] {
  val subProjections = List[Projection[_]](_1, _2, _3, _4, _5, _6, _7, _8, _9)
  def read(rs: ResultSet): Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9] =
    (_1.read(rs), _2.read(rs), _3.read(rs), _4.read(rs), _5.read(rs),
     _6.read(rs), _7.read(rs), _8.read(rs), _9.read(rs))
}

case class Tuple10Projection[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10] (
  val _1: Projection[T1],
  val _2: Projection[T2],
  val _3: Projection[T3],
  val _4: Projection[T4],
  val _5: Projection[T5],
  val _6: Projection[T6],
  val _7: Projection[T7],
  val _8: Projection[T8],
  val _9: Projection[T9],
  val _10: Projection[T10]
) extends CompositeProjection[Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]] {
  val subProjections = List[Projection[_]](_1, _2, _3, _4, _5, _6, _7, _8, _9, _10)
  def read(rs: ResultSet): Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10] =
    (_1.read(rs), _2.read(rs), _3.read(rs), _4.read(rs), _5.read(rs),
     _6.read(rs), _7.read(rs), _8.read(rs), _9.read(rs), _10.read(rs))
}

