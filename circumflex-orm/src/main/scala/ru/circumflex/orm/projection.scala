package ru.circumflex.orm

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
  private var _alias = "this"

  def read(rs: ResultSet) = ORM.typeConverter.read(rs, _alias).asInstanceOf[T]

  protected[orm] def alias = _alias
  def AS(alias: String): this.type = {
    this._alias = alias
    this
  }

  def sqlAliases = List(_alias)
}

/**
 * A contract for multi-column projections that should be read as a composite object.
 */
trait CompositeProjection[R] extends Projection[R] {

  private var _hash = 0

  val subProjections: Seq[Projection[_]]

  def sqlAliases = subProjections.flatMap(_.sqlAliases)

  override def equals(obj: Any) = obj match {
    case p: CompositeProjection[R] => this.subProjections == p.subProjections
    case _ => false
  }

  override def hashCode: Int = {
    if (_hash == 0)
      for (p <- subProjections) _hash = 31 * _hash + p.hashCode
    _hash
  }

  def toSql = subProjections.map(_.toSql).mkString(", ")
}

// ## Expression Projection

/**
 * This projection represents an arbitrary expression that RDBMS can understand
 * within `SELECT` clause (for example, `current_timestamp` or `count(*)`).
 */
class ExpressionProjection[T](val expression: String) extends AtomicProjection[T] {

  def toSql = ORM.dialect.alias(expression, alias)

  override def equals(obj: Any) = obj match {
    case p: ExpressionProjection[T] => p.expression == this.expression
    case _ => false
  }

  override def hashCode = expression.hashCode
}

// ## Field Projection

/**
 * A projection for single field of a record.
 */
class FieldProjection[R, T](val node: RelationNode[R], val field: Field[R, T]) extends AtomicProjection[T] {

  /**
   * Returns a column name qualified with node's alias.
   */
  def expr = ORM.dialect.qualifyColumn(field, node.alias)

  def toSql = ORM.dialect.alias(expr, alias)

  override def equals(obj: Any) = obj match {
    case p: FieldProjection[R, T] => p.node == this.node && p.field.name == this.field.name
    case _ => false
  }

  override def hashCode = node.hashCode * 31 + field.name.hashCode
}

// ## Record Projection

/**
 * A projection for reading entire `Record`.
 */
class RecordProjection[R](val node: RelationNode[R]) extends CompositeProjection[R] {
  private val log = ORM.getLogger(this)
  
  private type FP = FieldProjection[R, _]

  private val (pkProjection, otherProjections) = ((None: Option[FP], Nil: List[FP]) /: node.relation.fields) {(acc, f) =>
    val p = new FieldProjection(node, f)
    if (f == node.relation.PRIMARY_KEY)
      (Some(p), acc._2)
    else
      (acc._1, p :: acc._2)
  }
  
  private val isAutoPk = pkProjection match {
    case Some(p) if p.field.isInstanceOf[AutoPrimaryKeyField[R]] => true
    case _ => false
  }

  val subProjections = pkProjection match {
    case Some(p) => p :: otherProjections
    case None => otherProjections
  }

  def read(rs: ResultSet): R = {
    pkProjection match {
      case Some(p) =>
        p.read(rs) match {
          case id: Long =>
            // Has this record been cached? if true, should use and update it via rs instead of creae a new instance
            val record = node.relation.recordOf(id) getOrElse {
              val x = node.relation.recordClass.newInstance
              node.relation.updateCache(id, x)
              x
            }
            readRecord(rs, record)
          case x => 
            // there is case in left join, the joined table has none coresponding record, that is, a null id will be returned by p.read(rs)
            log.warning("null in read primary key of " + node.relation.recordClass.getSimpleName + ", which alias is '" + p.alias + "', value read is " + x)
            // @todo, return a null record or throws Exception?
            null.asInstanceOf[R]
        }
      case None => null.asInstanceOf[R]
    }
  }

  private def readRecord(rs: ResultSet, record: R): R = {
    query.recordsHolder += record
    val projections = if (isAutoPk) otherProjections else subProjections
    projections foreach {p =>
      p.field.setValue(record, p.read(rs).asInstanceOf[AnyRef]) match {
        case Some(lazyAction) => query.lazyFetchers += lazyAction
        case None =>
      }
    }
    // If record remains unidentified, do not return it.
    if (node.relation.transient_?(record)) null.asInstanceOf[R] else record
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

final case class Tuple2Projection[T1, T2] (
  val _1: Projection[T1],
  val _2: Projection[T2]
) extends CompositeProjection[Tuple2[T1, T2]] {
  val subProjections = List[Projection[_]](_1, _2)
  
  def read(rs: ResultSet): Tuple2[T1, T2] =
    (_1.read(rs), _2.read(rs))
}

final case class Tuple3Projection[T1, T2, T3] (
  val _1: Projection[T1],
  val _2: Projection[T2],
  val _3: Projection[T3]
) extends CompositeProjection[Tuple3[T1, T2, T3]] {
  val subProjections = List[Projection[_]](_1, _2, _3)

  def read(rs: ResultSet): Tuple3[T1, T2, T3] =
    (_1.read(rs), _2.read(rs), _3.read(rs))
}

final case class Tuple4Projection[T1, T2, T3, T4] (
  val _1: Projection[T1],
  val _2: Projection[T2],
  val _3: Projection[T3],
  val _4: Projection[T4]
) extends CompositeProjection[Tuple4[T1, T2, T3, T4]] {
  val subProjections = List[Projection[_]](_1, _2, _3, _4)

  def read(rs: ResultSet): Tuple4[T1, T2, T3, T4] =
    (_1.read(rs), _2.read(rs), _3.read(rs), _4.read(rs))
}

final case class Tuple5Projection[T1, T2, T3, T4, T5] (
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

final case class Tuple6Projection[T1, T2, T3, T4, T5, T6] (
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

final case class Tuple7Projection[T1, T2, T3, T4, T5, T6, T7] (
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

final case class Tuple8Projection[T1, T2, T3, T4, T5, T6, T7, T8] (
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

final case class Tuple9Projection[T1, T2, T3, T4, T5, T6, T7, T8, T9] (
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

final case class Tuple10Projection[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10] (
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

