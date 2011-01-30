package ru.circumflex.orm

import ORM._
import java.lang.String

// ## Relation Node

/**
 * **Relation Node** is essentially a wrapper around `Relation` that
 * provides an `alias` so that it can be used in SQL queries.
 */
abstract class RelationNode[R] extends SQLable with Cloneable {

  /**
   * Since we need to return a object in most case, let it to be deferred
   * @see Relation#as(String)
   */
  val relation: Relation[R]

  protected[orm] var _alias = "this"
  def alias = _alias
  def AS(alias: String): this.type = {
    this._alias = alias
    this
  }

  // ### Projections
  private lazy val wildcard = new RecordProjection[R](this)
  def * = wildcard

  // ### Criteria and simple queries

  def criteria = new Criteria(this)

  // ### Joins

  def findAssociation[F](node: RelationNode[F]): Option[Association[R, F]] =
    relation.findAssociation(node.relation)

  /**
   * Explicit join.
   */
  def JOIN[J](node: RelationNode[J], on: String, joinType: JoinType): JoinNode[R, J] =
    new ExplicitJoin(this, node, on, joinType)

  /**
   * Auto-join (the `ON` subclause is evaluated by searching matching association).
   */
  def JOIN[J](node: RelationNode[J], joinType: JoinType = LEFT): JoinNode[R, J] =
    findAssociation(node) match {
      case Some(a: Association[R, J]) =>  // many-to-one join
        new ManyToOneJoin[R, J](this, node, a, joinType)
      case _ =>
        node.findAssociation(this) match {
          case Some(a: Association[J, R]) =>  // one-to-many join
            new OneToManyJoin[R, J](this, node, a, joinType)
          case _ =>
            throw new ORMException("Failed to join " + this + " and " + node + ": no associations found.")
        }
    }

  // Join shortcuts for different types

  def INNER_JOIN[J <: Record[J]](node: RelationNode[J], on: String): JoinNode[R, J] =
    JOIN(node, on, INNER)
  def INNER_JOIN[J <: Record[J]](node: RelationNode[J]): JoinNode[R, J] =
    JOIN(node, INNER)

  def LEFT_JOIN[J <: Record[J]](node: RelationNode[J], on: String): JoinNode[R, J] =
    JOIN(node, on, LEFT)
  def LEFT_JOIN[J <: Record[J]](node: RelationNode[J]): JoinNode[R, J] =
    JOIN(node, LEFT)

  def RIGHT_JOIN[J <: Record[J]](node: RelationNode[J], on: String): JoinNode[R, J] =
    JOIN(node, on, RIGHT)
  def RIGHT_JOIN[J <: Record[J]](node: RelationNode[J]): JoinNode[R, J] =
    JOIN(node, RIGHT)

  def FULL_JOIN[J <: Record[J]](node: RelationNode[J], on: String): JoinNode[R, J] =
    JOIN(node, on, FULL)
  def FULL_JOIN[J <: Record[J]](node: RelationNode[J]): JoinNode[R, J] =
    JOIN(node, FULL)

  // ### Equality and others

  override def equals(obj: Any) = obj match {
    case r: RelationNode[_] => r.relation == this.relation && r.alias == this.alias
    case _ => false
  }

  override def hashCode = this.relation.hashCode * 31 + this.alias.hashCode

  /**
   * Creates a shallow copy of this node.
   * The underlying relation remains unchanged.
   */
  override def clone(): this.type = super.clone.asInstanceOf[this.type]

  def toSql = dialect.alias(relation.qualifiedName, alias)

  override def toString = toSql
}

// ## Proxy Node

/**
 * In order to organize joined nodes into tree we introduce this proxy
 * for `RelationNode`. It delegates all methods to underlying `node`.
 */
class ProxyNode[R](protected[orm] var node: RelationNode[R]) extends RelationNode[R] {
  val relation = node.relation
  override def alias = node.alias
  override def AS(alias: String): this.type = {
    node.AS(alias)
    this
  }

  override def * = node.*

  override def equals(obj: Any) = node.equals(obj)
  override def hashCode = node.hashCode

  override def toSql = node.toSql

  /**
   * Unlike `clone` in `RelationNode` this creates a deep copy
   * (clones underlying `node`, but `relation` remains unchanged).
   */
  override def clone(): this.type = {
    val newNode = super.clone().asInstanceOf[this.type]
    val n = node.clone().asInstanceOf[RelationNode[R]]
    newNode.node = n
    newNode
  }

}

// ## Joins

/**
 * This node represents a relational join between two nodes (`left` and `right`).
 */
abstract class JoinNode[L, R](
  protected var _left: RelationNode[L],
  protected var _right: RelationNode[R],
  protected var _joinType: JoinType
) extends ProxyNode[L](_left) {

  def left = _left
  def right = _right
  def joinType = _joinType

  /**
   * Join condition expression (used in `ON` subclauses).
   */
  def on: String

  def sqlOn = dialect.on(this.on)

  // ### Replacement methods

  def replaceLeft(newLeft: RelationNode[L]): this.type = {
    this._left = newLeft
    this
  }

  def replaceRight(newRight: RelationNode[R]): this.type = {
    this._right = newRight
    this
  }

  // ### Others

  override def toSql = dialect.join(this)

  /**
   * Creates a deep copy of this node, cloning left and right nodes.
   * The underlying relations of nodes remain unchanged.
   */
  override def clone(): this.type = {
    super.clone()
    .replaceLeft(this.left.clone)
    .replaceRight(this.right.clone)
  }

  override def toString = "(" + left + " -> " + right + ")"
}

/**
 * A join with explicit join condition.
 */
class ExplicitJoin[L, R](
  left: RelationNode[L],
  right: RelationNode[R],
  val on: String,
  joinType: JoinType
) extends JoinNode[L, R](left, right, joinType)

/**
 * A join in many-to-one direction.
 */
class ManyToOneJoin[L, R](
  childNode: RelationNode[L],
  parentNode: RelationNode[R],
  val association: Association[L, R],
  joinType: JoinType
) extends JoinNode[L, R](childNode, parentNode, joinType) {
  def on = childNode.alias + "." + association.name + " = " +
  parentNode.alias + "." + association.foreignRelation.PRIMARY_KEY.name
}

/**
 * A join in one-to-many direction.
 */
class OneToManyJoin[L, R](
  parentNode: RelationNode[L],
  childNode: RelationNode[R],
  val association: Association[R, L],
  joinType: JoinType
) extends JoinNode[L, R](parentNode, childNode, joinType) {
  def on = childNode.alias + "." + association.name + " = " +
  parentNode.alias + "." + association.foreignRelation.PRIMARY_KEY.name
}
