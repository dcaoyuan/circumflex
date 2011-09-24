package ru.circumflex.orm

import java.sql.ResultSet
import java.sql.PreparedStatement
import collection.mutable.ListBuffer
import java.sql.SQLException
import java.util.logging.Logger
import ru.circumflex.orm.avro.Avro
import ru.circumflex.orm.avro.AvroNode

// ## Query Commons

/**
 * The most common contract for queries.
 */
trait Query extends SQLable with ParameterizedExpression with Cloneable {
  protected val log = Logger.getLogger(this.getClass.getName)
  
  protected var aliasCounter = 0

  /**
   * Generate an alias to eliminate duplicates within query.
   */
  protected def nextAlias: String = {
    aliasCounter += 1
    "this_" + aliasCounter
  }

  /**
   * Set prepared statement parameters of this query starting from specified index.
   * Because `Query` objects can be nested, this method should return the new starting
   * index of prepared statement parameter.
   */
  def setParams(st: PreparedStatement, startIndex: Int): Int = {
    var paramsCounter = startIndex
    parameters foreach {p =>
      ORM.typeConverter.write(st, convertNamedParam(p), paramsCounter)
      paramsCounter += 1
    }
    paramsCounter
  }

  // Miscellaneous

  override def clone(): this.type = super.clone.asInstanceOf[this.type]

  // Named parameters

  protected var _namedParams: Map[String, Any] = Map()

  def renderParams: Seq[Any] = parameters.map(p => convertNamedParam(p))

  /**
   * Sets a named parameter value for this query.
   */
  def set(name: String, value: Any): this.type = {
    _namedParams += name -> value
    return this
  }
  def set(sym: Symbol, value: Any): this.type = set(sym.name, value)
  def update(name: String, value: Any): Unit = set(name, value)
  def update(sym: Symbol, value: Any): Unit = set(sym, value)

  protected def convertNamedParam(param: Any): Any = param match {
    case s: Symbol => lookupNamedParam(s.name)
    case s: String if s.startsWith(":") => lookupNamedParam(s)
    case _ => param
  }

  protected def lookupNamedParam(name: String): Any =
    _namedParams.get(name.replaceAll("^:", "")) match {
      case Some(p) => p
      case _ => name
    }

  override def toString = toSql
}

// ## SQL Queries

/**
 * A conrtact for SQL queries (data-retrieval). Specified `projection`
 * will be rendered in `SELECT` clause and will be used to read `ResultSet`.
 */
abstract class SQLQuery[T](val projection: Projection[T]) extends Query {

  ensureProjectionAlias(projection)

  /**
   * The `SELECT` clause of query. In normal circumstances this list should
   * only consist of single `projection` element; but if `GROUP_BY` clause
   * specifies projections that are not part of `projection`, than, they
   * are added here explicitly.
   */
  def projections: Seq[Projection[_]] = List(projection)

  /**
   * Make sure that projections with alias `this` are assigned query-unique alias.
   */
  protected def ensureProjectionAlias[_](projection: Projection[_]) {
    projection match {
      case x: AtomicProjection[_] if (x.alias == "this") => x.AS(nextAlias)
      case x: CompositeProjection[_] => x.subProjections foreach ensureProjectionAlias
      case _ =>
    }
  }

  // ### Data Retrieval Stuff

  protected[orm] val lazyFetchers = ListBuffer[() => Unit]()
  /**
   * A holder to hold strong references of records that were created during this query,
   * so these records won't be GCed and can be reached by lazyFetchers
   */
  protected[orm] val recordsHolder = ListBuffer[Any]()

  private def applyLazyFetchers {
    lazyFetchers foreach (_.apply())
    lazyFetchers.clear
    recordsHolder.clear
  }

  private def setProjectionQuery(projection: Projection[_]) {
    projection.query = this
    projection match {
      case x: CompositeProjection[_] => x.subProjections foreach setProjectionQuery
      case _ =>
    }
  }

  /**
   * Execute a query, open a JDBC `ResultSet` and executes specified `actions`.
   */
  @throws(classOf[SQLException])
  def resultSet[A](action: ResultSet => A): A = {

    projections foreach setProjectionQuery

    val result = query(action)

    // alway perform lazyFetchers after rs is processed completely to avoid nested sql query
    applyLazyFetchers

    result
  }

  @throws(classOf[SQLException])
  protected def query[A](postAction: ResultSet => A): A = {
    tx.executeOnce{conn =>
      val sql = toSql
      log.info(sql)

      val st = conn.prepareStatement(sql)
      setParams(st, 1)
      val rs = st.executeQuery
      val ret = postAction(rs)

      rs.close
      st.close
      ret
    } {ex => throw ex}
  }
  
  // ### Executors

  /**
   * Use the query projection to read 
   */
  def read(rs: ResultSet): T = projection.read(rs)

  /**
   * Execute a query and return `Seq[T]`, where `T` is designated by query projection.
   */
  @throws(classOf[SQLException])
  def list(): Seq[T] = resultSet{rs =>
    val result = new ListBuffer[T]()
    while (rs.next) result += read(rs)
    result
  }

  /**
   * Execute a query and return a unique result.
   *
   * An exception is thrown if result set yields more than one row.
   */
  @throws(classOf[SQLException])
  @throws(classOf[ORMException])
  def unique(): Option[T] = resultSet{rs =>
    if (!rs.next) None
    else if (rs.isLast) Some(read(rs))
    else throw new ORMException("Unique result expected, but multiple rows found.")
  }

  def toAvro(fileName: String) {
    projections foreach resetFieldProjectionAlias
    resultSet{rs =>
      Avro().write(fileName, rs, "anonymous")
    }
  }

  /**
   * Reset field alias to field name
   */
  protected def resetFieldProjectionAlias(projection: Projection[_]) {
    projection match {
      case x: FieldProjection[_, _] => x.AS(x.field.name)
      case x: CompositeProjection[_] => x.subProjections foreach resetFieldProjectionAlias
      case _ =>
    }
  }
}

// ## Native SQL

class NativeSQLQuery[T](projection: Projection[T],
                        expression: ParameterizedExpression
) extends SQLQuery[T](projection) {
  def parameters = expression.parameters
  def toSql = expression.toSql.replaceAll("\\{\\*\\}", projection.toSql)
}

// ## Full Select

/**
 * A full-fledged `SELECT` query.
 */
class Select[T]($projection: Projection[T]) extends SQLQuery[T]($projection) {

  protected var _auxProjections: Seq[Projection[_]] = Nil
  protected var _relationNodes: Seq[RelationNode[_]] = Nil
  protected var _where: Predicate = EmptyPredicate
  protected var _having: Predicate = EmptyPredicate
  protected var _groupBy: Seq[Projection[_]] = Nil
  protected var _setOps: Seq[Pair[SetOperation, SQLQuery[T]]] = Nil
  protected var _orders: Seq[Order] = Nil
  protected var _limit: Int = -1
  protected var _offset: Int = 0

  /**
   * Query parameters.
   */
  def parameters: Seq[Any] = (
    _where.parameters ++
    _having.parameters ++
    _setOps.flatMap(_._2.parameters) ++
    _orders.flatMap(_.parameters)
  )
  /**
   * Queries combined with this subselect using specific set operation
   * (pairs, `SetOperation -> Subselect`),
   */
  def setOps = _setOps

  /**
   * The `SELECT` clause of query.
   */
  override def projections = List(projection) ++ _auxProjections
  
  @throws(classOf[SQLException])
  override protected def query[A](postAction: ResultSet => A): A = {
    from.filter(_.isInstanceOf[AvroNode[_]]) match {
      case Seq() => super.query(postAction)
      case xs =>
        projections foreach resetFieldProjectionAlias
        val avroNode = xs.head
        val rs = avroNode.asInstanceOf[AvroNode[_]].fromAvro
        postAction(rs)
    }
  }

  def from = _relationNodes
  /**
   * Applies specified `nodes` as this query's `FROM` clause.
   * All nodes with `this` alias are assigned query-unique alias.
   */
  def FROM(nodes: RelationNode[_]*): Select[T] = {
    this._relationNodes = nodes
    from foreach ensureNodeAlias
    this
  }

  protected def ensureNodeAlias(node: RelationNode[_]): RelationNode[_] =
    node match {
      case x: JoinNode[_, _] =>
        ensureNodeAlias(x.left)
        ensureNodeAlias(x.right)
        x
      case x: RelationNode[_] if x.alias == "this" => node.AS(nextAlias)
      case x => x
    }

  def where: Predicate = _where
  def WHERE(predicate: Predicate): Select[T] = {
    this._where = predicate
    this
  }

  /**
   * Use specified `expression` as the `WHERE` clause of this query
   * with specified named `params`.
   */
  def WHERE(expression: String, params: Pair[String,Any]*): Select[T] =
    WHERE(prepareExpr(expression, params: _*))

  def having: Predicate = _having
  def HAVING(predicate: Predicate): Select[T] = {
    this._having = predicate
    this
  }

  /**
   * Use specified `expression` as the `HAVING` clause of this query
   * with specified named `params`.
   */
  def HAVING(expression: String, params: Pair[String,Any]*): Select[T] =
    HAVING(prepareExpr(expression, params: _*))

  def groupBy: Seq[Projection[_]] = _groupBy
  def GROUP_BY(proj: Projection[_]*): Select[T] = {
    proj foreach addGroupByProjection
    this
  }

  protected def addGroupByProjection(proj: Projection[_]) {
    findProjection(projection, p => p.equals(proj)) match {
      case None =>
        ensureProjectionAlias(proj)
        this._auxProjections ++= List(proj)
        this._groupBy ++= List(proj)
      case Some(p) => this._groupBy ++= List(p)
    }
  }

  /**
   * Search deeply for a projection that matches specified `predicate` function.
   */
  protected def findProjection(projection: Projection[_], predicate: Projection[_] => Boolean): Option[Projection[_]] = {
    if (predicate(projection)) Some(projection)
    else projection match {
      case p: CompositeProjection[_] => p.subProjections.find(predicate)
      case _ => None
    }
  }

  // ### Set Operations

  protected def addSetOp(op: SetOperation, sql: SQLQuery[T]): Select[T] = {
    val q = clone()
    q._setOps ++= List(op -> sql)
    q
  }

  def UNION(sql: SQLQuery[T]): Select[T] =
    addSetOp(OP_UNION, sql)

  def UNION_ALL(sql: SQLQuery[T]): Select[T] =
    addSetOp(OP_UNION_ALL, sql)

  def EXCEPT(sql: SQLQuery[T]): Select[T] =
    addSetOp(OP_EXCEPT, sql)

  def EXCEPT_ALL(sql: SQLQuery[T]): Select[T] =
    addSetOp(OP_EXCEPT_ALL, sql)

  def INTERSECT(sql: SQLQuery[T]): Select[T] =
    addSetOp(OP_INTERSECT, sql)

  def INTERSECT_ALL(sql: SQLQuery[T]): Select[T] =
    addSetOp(OP_INTERSECT_ALL, sql)

  def orderBy = _orders
  def ORDER_BY(order: Order*): Select[T] = {
    this._orders ++= order.toList
    this
  }

  def limit = _limit
  def LIMIT(value: Int): Select[T] = {
    _limit = value
    this
  }

  def offset = _offset
  def OFFSET(value: Int): Select[T] = {
    _offset = value
    this
  }

  def toSql = ORM.dialect.select(this)

}

// ## DML Queries

/**
 * A conrtact for DML queries (data-manipulation).
 */
trait DMLQuery extends Query {

  /**
   * Execute a query and return the number of affected rows.
   */
  def execute(): Int = tx.execute{conn =>
    val sql = toSql
    log.info(sql)
    
    val st = conn.prepareStatement(sql)
    setParams(st, 1)
    val rows = st.executeUpdate
      
    st.close
    rows
  } {ex => throw ex}
}

// ## Native DML

class NativeDMLQuery(expression: ParameterizedExpression) extends DMLQuery {
  def parameters = expression.parameters
  def toSql = expression.toSql
}

// ## INSERT-SELECT query

/**
 * Functionality for INSERT-SELECT query. Data extracted using specified `query`
 * and inserted into specified `relation`.
 *
 * The projections of `query` must match the columns of target `relation`.
 */
 class InsertSelect[R](val relation: Relation[R], val query: SQLQuery[_]) extends DMLQuery {
    if (relation.readOnly_?)
      throw new ORMException("The relation " + relation.qualifiedName + " is read-only.")
    def parameters = query.parameters
    def toSql: String = ORM.dialect.insertSelect(this)
  }

 /**
  * A lil helper to keep stuff DSL'ly.
  */
 class InsertSelectHelper[R](val relation: Relation[R]) {
    def SELECT[T](projection: Projection[T]) = new InsertSelect(relation, new Select(projection))
  }

// ## DELETE query

 /**
  * Functionality for DELETE query.
  */
 class Delete[R](val node: RelationNode[R]) extends DMLQuery {
    val relation = node.relation
    if (relation.readOnly_?)
      throw new ORMException("The relation " + relation.qualifiedName + " is read-only.")

    // ### WHERE clause

    protected var _where: Predicate = EmptyPredicate
    def where: Predicate = this._where
    def WHERE(predicate: Predicate): Delete[R] = {
      this._where = predicate
      this
    }

    // ### Miscellaneous
    def parameters = _where.parameters
    def toSql: String = ORM.dialect.delete(this)
  }

// ## UPDATE query

 /**
  * Functionality for UPDATE query.
  */
 class Update[R](val node: RelationNode[R]) extends DMLQuery {
    val relation = node.relation
    if (relation.readOnly_?)
      throw new ORMException("The relation " + relation.qualifiedName + " is read-only.")

    // ### SET clause

    private var _setClause: Seq[Pair[Field[R, _], Any]] = Nil
    def setClause = _setClause
    def SET[T](field: Field[R, _], value: Any): Update[R] = {
      _setClause ++= List(field -> value)
      this
    }
    def SET[F](association: Association[R, F], value: F): Update[R] =
      SET(association.field, association.foreignRelation.idOf(value))
    def SET_NULL[T](field: Field[R, _]): Update[R] = set(field, null.asInstanceOf[T])
    def SET_NULL[P](association: Association[R, P]): Update[R] =
      SET_NULL(association.field)

    // ### WHERE clause

    protected var _where: Predicate = EmptyPredicate
    def where: Predicate = _where
    def WHERE(predicate: Predicate): Update[R] = {
      this._where = predicate
      this
    }

    // ### Miscellaneous

    def parameters = _setClause.map(_._2) ++ _where.parameters
    def toSql: String = ORM.dialect.update(this)

  }



