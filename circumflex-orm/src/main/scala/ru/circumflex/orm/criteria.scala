package ru.circumflex.orm

// ## Criteria API

/**
 * **Criteria API** is a simplified queriyng interface which allows
 * to fetch records in neat object-oriented notation with the ability to
 * fetch the whole hierarchy of records in one query via *prefetching*.
 *
 * Criteria API is designed to operate on `Record`s specifically. If
 * you want to use different projections, use `Select` instead.
 */
class Criteria[R](val rootNode: RelationNode[R]) extends SQLable with Cloneable {

  private var _counter = 0
  protected def nextCounter: Int = {
    _counter += 1
    _counter
  }

  protected var _rootTree: RelationNode[R] = rootNode
  protected var _joinTree: RelationNode[R] = rootNode
  protected var _prefetchSeq: Seq[Association[_, _]] = Nil

  protected var _projections: Seq[RecordProjection[_]] = List(rootNode.*)
  protected var _restrictions: Seq[Predicate] = Nil
  protected var _orders: Seq[Order] = Nil

  protected var _limit: Int = -1
  protected var _offset: Int = 0

  // ### Internal stuff

  /**
   * Renumber specified `projection` aliases and it's `subProjections` recursively
   * so that no confusions happen.
   */
  protected def resetProjection(projection: Projection[_]): Unit = projection match {
    case x: AtomicProjection[_] => x.AS("p_" + nextCounter)
    case x: CompositeProjection[_] => x.subProjections foreach resetProjection
  }

  /**
   * Replace left-most node of specified `join` with specified `node`.
   */
  protected def replaceLeft(join: JoinNode[R, _], node: RelationNode[R]): RelationNode[R] =
    join.left match {
      case j: JoinNode[R, _] => replaceLeft(j, node)
      case r: RelationNode[R] => join.replaceLeft(node)
    }

  /**
   * Attempt to search the root tree of query plan for relations of specified `association`
   * and correspondingly update it if necessary.
   */
  protected def updateRootTree[N, P, C](node: RelationNode[N], association: Association[C, P]): RelationNode[N] =
    node match {
      case j: JoinNode[_, _] => j.replaceLeft(updateRootTree(j.left, association))
        .replaceRight(updateRootTree(j.right, association))
      case node: RelationNode[N] =>
        if (node.relation == association.relation) {   // N == C
          val a = association.asInstanceOf[Association[N, P]]
          new ManyToOneJoin(node, preparePf(a.foreignRelation, a), a, LEFT)
        } else if (node.relation == association.foreignRelation) {  // N == P
          val a = association.asInstanceOf[Association[C, N]]
          new OneToManyJoin(node, preparePf(a.relation, a), a, LEFT)
        } else node
    }

  /**
   * Prepare specified `node` and `association` to participate in prefetching.
   */
  protected def preparePf[N](relation: Relation[N], association: Association[_, _]): RelationNode[N] = {
    val node = relation.as("pf_" + nextCounter)
    _projections ++= List(node.*)
    _prefetchSeq ++= List[Association[_,_]](association)
    return node
  }

  /**
   * Perform a depth-search and add specified `node` to specified `tree` of joins.
   */
  protected def updateJoinTree[N](node: RelationNode[N],
                                  tree: RelationNode[R]
  ): RelationNode[R] =
    tree match {
      case x: JoinNode[R, R] => try {   // try the left side
          x.replaceLeft(updateJoinTree(node, x.left))
        } catch {
          case e: ORMException => x.replaceRight(updateJoinTree(node, x.right))  // try the right side
        }
      case x: RelationNode[R] => x.JOIN(node)
    }

  /**
   * Extract the information for inverse associations from specified `tuple` using
   * specified `tree`, which should appear to be a subtree of query plan.
   */
  protected def processTupleTree[N, P, C](tuple: Array[_], tree: RelationNode[N]): Unit =
    tree match {
      case x: OneToManyJoin[P, C] =>
        val pNode = x.left
        val cNode = x.right
        val a = x.association
        val pIndex = _projections.indexWhere(_.node.alias == pNode.alias)
        val cIndex = _projections.indexWhere(_.node.alias == cNode.alias)
        if (pIndex == -1 || cIndex == -1) return
        val parent = tuple(pIndex).asInstanceOf[P]
        val child  = tuple(cIndex).asInstanceOf[C]
        if (null != parent) {
          var children = tx.getCachedInverse(parent, a) match {
            case null => Nil
            case l: Seq[C] => l
          }
          if (null != child && !children.contains(child))
            children ++= List(child)
          tx.updateInverseCache(parent, a, children)
        }
        processTupleTree(tuple, x.left)
        processTupleTree(tuple, x.right)
      case x: JoinNode[_, _] =>
        processTupleTree(tuple, x.left)
        processTupleTree(tuple, x.right)
      case _ =>
    }
  
  protected def prepareLimitOffsetPredicate: Predicate = {
    val n = rootNode.clone.AS("__lo")
    val q = SELECT (n.id) FROM (n) LIMIT (_limit) OFFSET (_offset) ORDER_BY (_orders: _*)
    rootNode.id IN (q)  
  }

  // ## Public Stuff

  /**
   * Add specified `predicates` to restrictions list.
   */
  def add(predicates: Predicate*): Criteria[R] = {
    _restrictions ++= predicates.toList
    this
  }
  def add(expression: String, params: Pair[String, Any]*): Criteria[R] =
    add(prepareExpr(expression, params: _*))

  /**
   * Add specified `orders` to order specificators list.
   */
  def addOrder(orders: Order*): Criteria[R] = {
    _orders ++= orders.toList
    this
  }
  /**
   * Set the maximum amount of root records that will be returned by the query.
   */
  def limit(value: Int): Criteria[R] = {
    this._limit = value
    this
  }

  /**
   * Set the offset for root records that will be returned by the query.
   */
  def offset(value: Int): Criteria[R] = {
    this._offset = value
    this
  }

  /**
   * Add specified `association` to prefetch list.
   */
  def prefetch[P, C](association: Association[C, P]): Criteria[R] = {
    if (!_prefetchSeq.contains(association)) {
      // The depth-search is used to update query plan if possible.
      _rootTree = updateRootTree(_rootTree, association)
      // TODO also process prefetch list of both sides of association.
    }
    this
  }

  /**
   * Add specified `node` to join tree so that you can build queries with transitive criteria.
   */
  def addJoin[N](node: RelationNode[N]): Criteria[R] = {
    _joinTree = updateJoinTree(node, _joinTree)
    this
  }

  /**
   * Make an SQL SELECT query from this criteria.
   */
  def mkSelect: SQLQuery[Array[Any]] ={
    SELECT(new UntypedTupleProjection(projections: _*))
    .FROM(queryPlan)
    .WHERE(predicate)
    .ORDER_BY(_orders: _*)
  }

  /**
   * Make a DML `UPDATE` query from this criteria. Only `WHERE` clause is used, all the
   * other stuff is ignored.
   */
  def mkUpdate: Update[R] = UPDATE(rootNode).WHERE(predicate)

  /**
   * Make a DML `DELETE` query from this criteria. Only `WHERE` clause is used, all the
   * other stuff is ignored.
   */
  def mkDelete: Delete[R] = DELETE(rootNode).WHERE(predicate)

  /**
   * Renumber the aliases of all projections so that no confusions happen.
   */
  def projections: Seq[Projection[_]] = {
    _projections foreach resetProjection
    _projections
  }

  /**
   * Compose an actual predicate from restrictions.
   */
  def predicate: Predicate = {
    if (_limit != -1 || _offset != 0)
      add(prepareLimitOffsetPredicate)
    if (_restrictions.size > 0)
      AND(_restrictions: _*)
    else EmptyPredicate
  }

  /**
   * Merge the *join tree* with *prefetch tree* to form an actual `FROM` clause.
   */
  def queryPlan: RelationNode[R] = _joinTree match {
    case x: JoinNode[R, _] => replaceLeft(x.clone, _rootTree)
    case _: RelationNode[R] => _rootTree
  }

  /**
   * Execute a query, process prefetches and retrieve the list of records.
   */
  def list: Seq[R] = {
    val q = mkSelect
    q.resultSet{rs =>
      var result: Seq[R] = Nil
      while (rs.next) {
        val tuple = q.read(rs)
        processTupleTree(tuple, _rootTree)
        val root = tuple(0).asInstanceOf[R]
        if (!result.contains(root))
          result ++= List(root)
      }
      return result
    }
  }

  /**
   * Execute a query, process prefetches and retrieve unique root record. If result set
   * yields multiple root records, an exception is thrown.
   */
  def unique: Option[R] = {
    val q = mkSelect
    q.resultSet{rs =>
      if (!rs.next) return None     // none records found
      // Okay, let's grab the first one. This would be the result eventually.
      val firstTuple = q.read(rs)
      processTupleTree(firstTuple, _rootTree)
      val result = firstTuple(0).asInstanceOf[R]
      if (result == null) return None
      // We don't want to screw prefetches up so let's walk till the end,
      // but make sure that no other root records appear in result set.
      while (rs.next) {
        val tuple = q.read(rs)
        processTupleTree(tuple, _rootTree)
        val root = tuple.apply(0)
        if (root != result)   // Wow, this thingy shouldn't be here, call the police!
          throw new ORMException("Unique result expected, but multiple records found.")
      }
      return Some(result)
    }
  }

  // ### Miscellaneous

  def toSql = mkSelect.toSql

  override def toString = queryPlan.toString
}
